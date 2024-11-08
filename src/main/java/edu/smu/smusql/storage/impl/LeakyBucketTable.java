package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import java.util.*;

public class LeakyBucketTable implements Table {
    private final List<String> columns;
    private final int MAIN_CAPACITY = 1000;  // Size of main bucket
    private final int LEAK_THRESHOLD = 800;  // When to start leaking

    private Bucket mainBucket;      // Active data
    private Bucket overflowBucket;  // "Leaked" data
    private int operationCount = 0; // Track operations for cleanup timing

    private class Bucket {
        List<Map<String, String>> rows;
        Set<Integer> deletedRows;  // Track indices of deleted rows
        int virtualSize;           // Size including deleted rows

        Bucket() {
            rows = new ArrayList<>();
            deletedRows = new HashSet<>();
            virtualSize = 0;
        }

        void add(Map<String, String> row) {
            // Reuse deleted slots if available
            if (!deletedRows.isEmpty()) {
                int slot = deletedRows.iterator().next();
                deletedRows.remove(slot);
                rows.set(slot, row);
            } else {
                rows.add(row);
            }
            virtualSize++;
        }

        boolean delete(int index) {
            if (index >= 0 && index < rows.size() && !deletedRows.contains(index)) {
                deletedRows.add(index);
                virtualSize--;
                return true;
            }
            return false;
        }

        boolean isFull() {
            return virtualSize >= MAIN_CAPACITY;
        }

        void compact() {
            if (deletedRows.isEmpty()) return;

            List<Map<String, String>> newRows = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                if (!deletedRows.contains(i)) {
                    newRows.add(rows.get(i));
                }
            }
            rows = newRows;
            deletedRows.clear();
            virtualSize = rows.size();
        }
    }

    public LeakyBucketTable(List<String> columns) {
        this.columns = new ArrayList<>(columns);
        this.mainBucket = new Bucket();
        this.overflowBucket = new Bucket();
    }

    @Override
    public void insert(List<String> values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }

        // If main bucket is getting full, leak some data
        if (mainBucket.virtualSize >= LEAK_THRESHOLD) {
            leakData();
        }

        // Always insert to main bucket
        mainBucket.add(row);

        operationCount++;
        checkCleanup();
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();

        // Check main bucket first
        for (int i = 0; i < mainBucket.rows.size(); i++) {
            if (!mainBucket.deletedRows.contains(i)) {
                Map<String, String> row = mainBucket.rows.get(i);
                if (evaluateConditions(row, conditions)) {
                    results.add(new HashMap<>(row));
                }
            }
        }

        // Check overflow bucket if not too busy
        if (operationCount % 10 != 0) {  // Skip overflow sometimes when busy
            for (int i = 0; i < overflowBucket.rows.size(); i++) {
                if (!overflowBucket.deletedRows.contains(i)) {
                    Map<String, String> row = overflowBucket.rows.get(i);
                    if (evaluateConditions(row, conditions)) {
                        results.add(new HashMap<>(row));
                        // Move frequently accessed overflow data back to main
                        if (random.nextInt(100) < 10) {  // 10% chance
                            promoteRow(row);
                        }
                    }
                }
            }
        }

        operationCount++;
        checkCleanup();
        return results;
    }

    @Override
    public int update(String column, String value, List<String[]> conditions) {
        int count = 0;

        // Update main bucket
        for (int i = 0; i < mainBucket.rows.size(); i++) {
            if (!mainBucket.deletedRows.contains(i)) {
                Map<String, String> row = mainBucket.rows.get(i);
                if (evaluateConditions(row, conditions)) {
                    row.put(column, value);
                    count++;
                }
            }
        }

        // Update overflow if not too busy
        if (operationCount % 5 != 0) {  // Skip overflow sometimes
            for (int i = 0; i < overflowBucket.rows.size(); i++) {
                if (!overflowBucket.deletedRows.contains(i)) {
                    Map<String, String> row = overflowBucket.rows.get(i);
                    if (evaluateConditions(row, conditions)) {
                        row.put(column, value);
                        count++;
                    }
                }
            }
        }

        operationCount++;
        checkCleanup();
        return count;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int count = 0;

        // Delete from main bucket
        for (int i = 0; i < mainBucket.rows.size(); i++) {
            if (!mainBucket.deletedRows.contains(i)) {
                Map<String, String> row = mainBucket.rows.get(i);
                if (evaluateConditions(row, conditions)) {
                    mainBucket.delete(i);
                    count++;
                }
            }
        }

        // Delete from overflow if not too busy
        if (operationCount % 5 != 0) {
            for (int i = 0; i < overflowBucket.rows.size(); i++) {
                if (!overflowBucket.deletedRows.contains(i)) {
                    Map<String, String> row = overflowBucket.rows.get(i);
                    if (evaluateConditions(row, conditions)) {
                        overflowBucket.delete(i);
                        count++;
                    }
                }
            }
        }

        operationCount++;
        checkCleanup();
        return count;
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    private final Random random = new Random();

    private void leakData() {
        // Move older data to overflow
        int toMove = mainBucket.virtualSize - (MAIN_CAPACITY / 2);
        int moved = 0;

        for (int i = 0; i < mainBucket.rows.size() && moved < toMove; i++) {
            if (!mainBucket.deletedRows.contains(i)) {
                overflowBucket.add(mainBucket.rows.get(i));
                mainBucket.delete(i);
                moved++;
            }
        }
    }

    private void promoteRow(Map<String, String> row) {
        if (!mainBucket.isFull()) {
            mainBucket.add(new HashMap<>(row));
        }
    }

    private void checkCleanup() {
        if (operationCount >= 1000) {
            mainBucket.compact();
            overflowBucket.compact();
            operationCount = 0;
        }
    }

    // Same evaluateConditions and evaluateCondition methods as before
    private boolean evaluateConditions(Map<String, String> row, List<String[]> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return true;
        }

        boolean result = true;
        boolean isOr = false;

        for (String[] condition : conditions) {
            if (condition[0] != null) { // AND/OR
                if (condition[0].equals("OR")) {
                    isOr = true;
                }
                continue;
            }

            String column = condition[1];
            String operator = condition[2];
            String value = condition[3];
            boolean matches = evaluateCondition(row.get(column), operator, value);

            if (isOr) {
                result = result || matches;
                isOr = false;
            } else {
                result = result && matches;
            }
        }

        return result;
    }

    private boolean evaluateCondition(String rowValue, String operator, String value) {
        try {
            double rowNum = Double.parseDouble(rowValue);
            double valueNum = Double.parseDouble(value);

            return switch (operator) {
                case "=" -> rowNum == valueNum;
                case ">" -> rowNum > valueNum;
                case "<" -> rowNum < valueNum;
                case ">=" -> rowNum >= valueNum;
                case "<=" -> rowNum <= valueNum;
                default -> false;
            };
        } catch (NumberFormatException e) {
            return switch (operator) {
                case "=" -> rowValue.equals(value);
                default -> false;
            };
        }
    }
}