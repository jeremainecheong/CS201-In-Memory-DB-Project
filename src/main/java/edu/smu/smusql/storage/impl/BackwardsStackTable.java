package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import java.util.*;

public class BackwardsStackTable implements Table {
    private final List<String> columns;
    private Stack<Map<String, String>> rows;

    public BackwardsStackTable(List<String> columns) {
        this.columns = new ArrayList<>(columns);
        this.rows = new Stack<>();
    }

    @Override
    public void insert(List<String> values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }
        rows.push(row);
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();
        Stack<Map<String, String>> tempStack = new Stack<>();
        boolean matchAny = false; // For OR conditions

        // Pop everything (backwards access pattern)
        while (!rows.isEmpty()) {
            Map<String, String> row = rows.pop();
            boolean matches = evaluateConditions(row, conditions);
            if (matches) {
                results.add(new HashMap<>(row));
            }
            tempStack.push(row);
        }

        // Push everything back
        while (!tempStack.isEmpty()) {
            rows.push(tempStack.pop());
        }

        return results;
    }

    @Override
    public int update(String column, String value, List<String[]> conditions) {
        int count = 0;
        Stack<Map<String, String>> tempStack = new Stack<>();

        while (!rows.isEmpty()) {
            Map<String, String> row = rows.pop();
            if (evaluateConditions(row, conditions)) {
                row.put(column, value);
                count++;
            }
            tempStack.push(row);
        }

        while (!tempStack.isEmpty()) {
            rows.push(tempStack.pop());
        }

        return count;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int count = 0;
        Stack<Map<String, String>> tempStack = new Stack<>();

        while (!rows.isEmpty()) {
            Map<String, String> row = rows.pop();
            if (!evaluateConditions(row, conditions)) {
                tempStack.push(row);
            } else {
                count++;
            }
        }

        while (!tempStack.isEmpty()) {
            rows.push(tempStack.pop());
        }

        return count;
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columns);
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