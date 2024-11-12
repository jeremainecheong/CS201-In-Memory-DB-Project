package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import java.util.*;

public class PingPongTable implements Table {
    private final List<String> columns;
    private List<Map<String, String>> activeList;    // "Hot" data
    private List<Map<String, String>> inactiveList;  // "Cold" data
    private Map<Map<String, String>, Integer> accessCounts;  // Track access frequency
    private static final int PING_PONG_THRESHOLD = 5000;  // More realistic threshold
    private static final int HOT_ACCESS_THRESHOLD = 3;    // Number of accesses to consider data "hot"
    private int operationCount = 0;

    public PingPongTable(List<String> columns) {
        this.columns = new ArrayList<>(columns);
        this.activeList = new ArrayList<>();
        this.inactiveList = new ArrayList<>();
        this.accessCounts = new HashMap<>();
    }

    @Override
    public void insert(List<String> values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }

        // New data starts in inactive list
        inactiveList.add(row);
        accessCounts.put(row, 1);

        checkAndSwap();
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();

        // Check active list first
        for (Map<String, String> row : activeList) {
            if (evaluateConditions(row, conditions)) {
                results.add(new HashMap<>(row));
                incrementAccess(row);
            }
        }

        // Then check inactive list
        List<Map<String, String>> toPromote = new ArrayList<>();
        for (Map<String, String> row : inactiveList) {
            if (evaluateConditions(row, conditions)) {
                results.add(new HashMap<>(row));
                incrementAccess(row);
                
                // If row is frequently accessed, mark for promotion
                if (accessCounts.get(row) >= HOT_ACCESS_THRESHOLD) {
                    toPromote.add(row);
                }
            }
        }

        // Promote frequently accessed rows to active list
        if (!toPromote.isEmpty()) {
            inactiveList.removeAll(toPromote);
            activeList.addAll(toPromote);
        }

        checkAndSwap();
        return results;
    }

    @Override
    public int update(String column, String value, List<String[]> conditions) {
        int count = 0;
        
        // Update active list
        for (Map<String, String> row : activeList) {
            if (evaluateConditions(row, conditions)) {
                row.put(column, value);
                count++;
                incrementAccess(row);
            }
        }

        // Update inactive list
        List<Map<String, String>> toPromote = new ArrayList<>();
        for (Map<String, String> row : inactiveList) {
            if (evaluateConditions(row, conditions)) {
                row.put(column, value);
                count++;
                incrementAccess(row);
                
                // If row is frequently accessed, mark for promotion
                if (accessCounts.get(row) >= HOT_ACCESS_THRESHOLD) {
                    toPromote.add(row);
                }
            }
        }

        // Promote frequently accessed rows
        if (!toPromote.isEmpty()) {
            inactiveList.removeAll(toPromote);
            activeList.addAll(toPromote);
        }

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
                accessCounts.remove(row);
                count++;
            }
        }

        // Delete from inactive list
        Iterator<Map<String, String>> inactiveIter = inactiveList.iterator();
        while (inactiveIter.hasNext()) {
            Map<String, String> row = inactiveIter.next();
            if (evaluateConditions(row, conditions)) {
                inactiveIter.remove();
                accessCounts.remove(row);
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

    private void incrementAccess(Map<String, String> row) {
        accessCounts.merge(row, 1, Integer::sum);
    }

    // Check if we should reorganize the lists
    private void checkAndSwap() {
        operationCount++;
        if (operationCount >= PING_PONG_THRESHOLD) {
            // Reset access counts and reorganize data
            Map<Map<String, String>, Integer> newAccessCounts = new HashMap<>();
            List<Map<String, String>> newActive = new ArrayList<>();
            List<Map<String, String>> newInactive = new ArrayList<>();

            // Process active list
            for (Map<String, String> row : activeList) {
                int count = accessCounts.get(row);
                // Decay access count but maintain some history
                int newCount = Math.max(1, count / 2);
                newAccessCounts.put(row, newCount);
                
                if (newCount >= HOT_ACCESS_THRESHOLD) {
                    newActive.add(row);
                } else {
                    newInactive.add(row);
                }
            }

            // Process inactive list
            for (Map<String, String> row : inactiveList) {
                int count = accessCounts.get(row);
                // Decay access count but maintain some history
                int newCount = Math.max(1, count / 2);
                newAccessCounts.put(row, newCount);
                
                if (newCount >= HOT_ACCESS_THRESHOLD) {
                    newActive.add(row);
                } else {
                    newInactive.add(row);
                }
            }

            activeList = newActive;
            inactiveList = newInactive;
            accessCounts = newAccessCounts;
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
