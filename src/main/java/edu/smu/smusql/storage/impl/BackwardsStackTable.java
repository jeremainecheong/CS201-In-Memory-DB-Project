package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.DataType;

import java.util.*;

/**
 * BackwardsStackTable implements a stack-based storage structure
 * optimized for Last-In-First-Out operations while ensuring test compliance.
 */
public class BackwardsStackTable implements Table {
    private final List<String> columnNames;
    private final Stack<DataType[]> stack; // Keeping the stack structure

    /**
     * Constructs a BackwardsStackTable with the specified columns.
     *
     * @param columns List of column names.
     */
    public BackwardsStackTable(List<String> columns) {
        this.columnNames = new ArrayList<>(columns);
        this.stack = new Stack<>();
    }

    @Override
    public void insert(List<String> values) {
        if (values.size() != columnNames.size()) {
            throw new IllegalArgumentException("Value count mismatch");
        }
        DataType[] row = createRow(values);
        stack.push(row); // Push the row onto the stack
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();

        // Iterate from bottom to top to maintain insertion (FIFO) order
        for (int i = 0; i < stack.size(); i++) {
            DataType[] row = stack.get(i);
            if (matchesConditions(row, conditions)) {
                results.add(rowToMap(row));
            }
        }

        return results;
    }

    @Override
    public int update(String column, String newValue, List<String[]> conditions) {
        int updateCount = 0;
        int columnIndex = columnNames.indexOf(column);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        DataType newDataValue = newValue == null ? null : DataType.fromString(newValue);

        // Iterate from bottom to top to maintain insertion (FIFO) order
        for (int i = 0; i < stack.size(); i++) {
            DataType[] row = stack.get(i);
            if (matchesConditions(row, conditions)) {
                row[columnIndex] = newDataValue;
                updateCount++;
            }
        }

        return updateCount;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int deleteCount = 0;
        // Use an iterator to safely remove elements while iterating
        Iterator<DataType[]> iterator = stack.iterator();

        while (iterator.hasNext()) {
            DataType[] row = iterator.next();
            if (matchesConditions(row, conditions)) {
                iterator.remove();
                deleteCount++;
            }
        }

        return deleteCount;
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columnNames);
    }

    /**
     * Converts a row's DataType array into a Map for easy representation.
     */
    private Map<String, String> rowToMap(DataType[] row) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(columnNames.get(i), row[i] != null ? row[i].toString() : "null");
        }
        return map;
    }

    /**
     * Creates a DataType array from the list of string values.
     */
    private DataType[] createRow(List<String> values) {
        DataType[] row = new DataType[columnNames.size()];
        for (int i = 0; i < values.size(); i++) {
            row[i] = values.get(i) != null && !values.get(i).equalsIgnoreCase("null")
                    ? DataType.fromString(values.get(i))
                    : null;
        }
        return row;
    }

    private boolean matchesConditions(DataType[] row, List<String[]> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return true;
        }

        // Special case: if first condition is OR, treat it differently
        if (conditions.get(0)[0].equals("OR")) {
            return handleOrOnlyConditions(row, conditions);
        }

        boolean finalResult = false;
        boolean currentAndGroup = true;

        for (String[] condition : conditions) {
            if (condition.length < 4) {
                throw new IllegalArgumentException("Invalid condition format");
            }

            String logicalOp = condition[0].toUpperCase();
            String column = condition[1];
            String operator = condition[2];
            String value = condition[3];

            boolean conditionResult = evaluateCondition(row, column, operator, value);

            if (logicalOp.equals("OR")) {
                // Save result of previous AND group
                finalResult = finalResult || currentAndGroup;
                // Start new AND group
                currentAndGroup = true;
                // Apply current condition to new group
                currentAndGroup = conditionResult;
            } else { // AND
                currentAndGroup = currentAndGroup && conditionResult;
            }
        }

        // Don't forget to include the last AND group
        return finalResult || currentAndGroup;
    }

    // Handle case where all conditions are OR
    private boolean handleOrOnlyConditions(DataType[] row, List<String[]> conditions) {
        for (String[] condition : conditions) {
            if (!condition[0].equals("OR")) {
                throw new IllegalArgumentException("Mixed AND/OR not allowed in this context");
            }

            String column = condition[1];
            String operator = condition[2];
            String value = condition[3];

            if (evaluateCondition(row, column, operator, value)) {
                return true;  // Any single OR condition being true is enough
            }
        }
        return false;  // None of the OR conditions were true
    }


    /**
     * Evaluates a single condition against a row.
     */
    private boolean evaluateCondition(DataType[] row, String column, String operator, String value) {
        int columnIndex = columnNames.indexOf(column);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        DataType rowValue = row[columnIndex];
        if (rowValue == null) return false;

        DataType compareValue = DataType.fromString(value);

        return switch (operator) {
            case "=" -> rowValue.equals(compareValue);
            case ">" -> rowValue.compareTo(compareValue) > 0;
            case "<" -> rowValue.compareTo(compareValue) < 0;
            case ">=" -> rowValue.compareTo(compareValue) >= 0;
            case "<=" -> rowValue.compareTo(compareValue) <= 0;
            default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
        };
    }
}
