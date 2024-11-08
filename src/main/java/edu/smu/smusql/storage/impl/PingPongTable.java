package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import java.util.*;

public class PingPongTable implements Table {
    private final List<String> columns;
    private List<Map<String, String>> activeList;    // Currently "hot" data
    private List<Map<String, String>> inactiveList;  // Currently "cold" data
    private int pingPongThreshold = 1000;            // When to switch lists
    private int operationCount = 0;

    public PingPongTable(List<String> columns) {
        this.columns = new ArrayList<>(columns);
        this.activeList = new ArrayList<>();
        this.inactiveList = new ArrayList<>();
    }

    @Override
    public void insert(List<String> values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }

        // New data always goes to active list
        activeList.add(row);
        checkAndSwap();
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();
        Set<Map<String, String>> matched = new HashSet<>();  // Track matches for bouncing

        // Check active list first (likely to find matches here)
        for (Map<String, String> row : activeList) {
            if (evaluateConditions(row, conditions)) {
                results.add(new HashMap<>(row));
                matched.add(row);
            }
        }

        // Then check inactive list
        for (Map<String, String> row : inactiveList) {
            if (evaluateConditions(row, conditions)) {
                results.add(new HashMap<>(row));
                matched.add(row);
                // Bounce matching rows to active list (they're being used)
                activeList.add(row);
            }
        }

        // Remove bounced rows from inactive
        inactiveList.removeAll(matched);

        checkAndSwap();
        return results;
    }

    @Override
    public int update(String column, String value, List<String[]> conditions) {
        int count = 0;
        Set<Map<String, String>> matched = new HashSet<>();

        // Update in active list
        for (Map<String, String> row : activeList) {
            if (evaluateConditions(row, conditions)) {
                row.put(column, value);
                count++;
                matched.add(row);
            }
        }

        // Update in inactive list and bounce updated rows
        for (Map<String, String> row : inactiveList) {
            if (evaluateConditions(row, conditions)) {
                row.put(column, value);
                count++;
                matched.add(row);
                activeList.add(row);
            }
        }

        inactiveList.removeAll(matched);
        checkAndSwap();
        return count;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int count = 0;

        // Delete from active list
        Iterator<Map<String, String>> activeIter = activeList.iterator();
        while (activeIter.hasNext()) {
            Map<String, String> row = activeIter.next();
            if (evaluateConditions(row, conditions)) {
                activeIter.remove();
                count++;
            }
        }

        // Delete from inactive list
        Iterator<Map<String, String>> inactiveIter = inactiveList.iterator();
        while (inactiveIter.hasNext()) {
            Map<String, String> row = inactiveIter.next();
            if (evaluateConditions(row, conditions)) {
                inactiveIter.remove();
                count++;
            }
        }

        checkAndSwap();
        return count;
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    // Check if we should swap active/inactive lists
    private void checkAndSwap() {
        operationCount++;
        if (operationCount >= pingPongThreshold) {
            // Time to swap! Move older active data to inactive
            List<Map<String, String>> newActive = new ArrayList<>();
            // Keep most recent 20% in active
            int keepCount = Math.max(1, activeList.size() / 5);
            for (int i = activeList.size() - keepCount; i < activeList.size(); i++) {
                newActive.add(activeList.get(i));
            }
            // Move rest to inactive
            for (int i = 0; i < activeList.size() - keepCount; i++) {
                inactiveList.add(activeList.get(i));
            }
            activeList = newActive;
            operationCount = 0;
        }
    }

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