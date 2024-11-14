package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.DataType;

import java.util.*;

/**
 * RandomQueueTable implements a multi-queue storage structure
 * optimized for distributing data across multiple queues to balance load.
 * Each row is represented as a DataType array, aligning with column indices.
 */
public class RandomQueueTable implements Table {
    private final List<String> columns;
    private final List<Queue<DataType[]>> queues; // Multiple queues using LinkedList
    private static final int NUM_QUEUES = 4; // Number of queues to distribute data
    private static final int REBALANCE_THRESHOLD = 5000; // Operation count threshold for rebalancing
    private static final double SIZE_IMBALANCE_THRESHOLD = 2.0; // Rebalance if max size > min size * threshold
    private int operationCount = 0;
    private int totalRows = 0;
    private int currentQueue = 0; // Counter for round-robin distribution

    /**
     * Constructs a RandomQueueTable with the specified columns.
     *
     * @param columns List of column names.
     */
    public RandomQueueTable(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns list cannot be null or empty.");
        }
        this.columns = new ArrayList<>(columns);
        this.queues = new ArrayList<>(NUM_QUEUES);
        for (int i = 0; i < NUM_QUEUES; i++) {
            queues.add(new LinkedList<>());
        }
    }

    @Override
    public void insert(List<String> values) {
        validateValues(values);

        DataType[] row = createRow(values);

        // Round-robin distribution
        int queueIndex = currentQueue % NUM_QUEUES;
        queues.get(queueIndex).offer(row);
        currentQueue++;

        totalRows++;

        incrementOperationCount();

        if (shouldRebalance()) {
            rebalanceQueues();
        }
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<DataType[]> matchedRows = new ArrayList<>();

        // Collect all matching rows from all queues
        for (Queue<DataType[]> queue : queues) {
            for (DataType[] row : queue) {
                if (evaluateConditions(row, conditions)) {
                    matchedRows.add(row);
                }
            }
        }

        // Remove sorting to preserve insertion order
//         sortRowsById(matchedRows);

        // Convert the matched DataType[] rows to Map<String, String>
        List<Map<String, String>> results = new ArrayList<>();
        for (DataType[] row : matchedRows) {
            results.add(rowToMap(row));
        }

        incrementOperationCount();

        return results;
    }

    @Override
    public int update(String column, String value, List<String[]> conditions) {
        int columnIndex = columns.indexOf(column);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        DataType newDataValue = value == null ? null : DataType.fromString(value);
        int updatedCount = 0;

        for (Queue<DataType[]> queue : queues) {
            for (DataType[] row : queue) {
                if (evaluateConditions(row, conditions)) {
                    row[columnIndex] = newDataValue;
                    updatedCount++;
                }
            }
        }

        incrementOperationCount();

        return updatedCount;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int deletedCount = 0;

        for (Queue<DataType[]> queue : queues) {
            Iterator<DataType[]> iterator = queue.iterator();
            while (iterator.hasNext()) {
                DataType[] row = iterator.next();
                if (evaluateConditions(row, conditions)) {
                    iterator.remove();
                    deletedCount++;
                    totalRows--;
                }
            }
        }

        // Trigger rebalance if a significant number of rows have been deleted
        if (deletedCount > totalRows / 4 && totalRows > 0) {
            rebalanceQueues();
        }

        incrementOperationCount();

        return deletedCount;
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    /**
     * Converts a row's DataType array into a Map for easy representation.
     *
     * @param row The DataType array representing the row.
     * @return A Map with column names as keys and their corresponding string values.
     */
    private Map<String, String> rowToMap(DataType[] row) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            DataType data = row[i];
            map.put(columns.get(i), data != null ? data.toString() : "null");
        }
        return map;
    }

    /**
     * Creates a DataType array from the list of string values.
     *
     * @param values List of string values.
     * @return A DataType array representing the row.
     */
    private DataType[] createRow(List<String> values) {
        DataType[] row = new DataType[columns.size()];
        for (int i = 0; i < values.size(); i++) {
            row[i] = values.get(i) != null && !values.get(i).equalsIgnoreCase("null")
                    ? DataType.fromString(values.get(i))
                    : null;
        }
        return row;
    }

    /**
     * Determines the queue index for a given row based on round-robin distribution.
     *
     * @param row The DataType array representing the row.
     * @return The index of the queue where the row should be placed.
     */
    // No longer using hash-based distribution
    // Removed getQueueIndex method related to hashing
    // Distribution is handled via round-robin in insert()

    /**
     * Evaluates if a row matches the provided conditions.
     *
     * @param row        The DataType array representing the row.
     * @param conditions List of conditions.
     * @return True if the row matches the conditions, false otherwise.
     */
    private boolean evaluateConditions(DataType[] row, List<String[]> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return true; // No conditions mean all rows match
        }

        boolean result = false;
        boolean isFirstCondition = true;

        for (String[] condition : conditions) {
            if (condition.length < 4) {
                throw new IllegalArgumentException("Invalid condition format. Each condition must have at least 4 elements.");
            }

            String logicalOp = condition[0];
            String column = condition[1];
            String operator = condition[2];
            String value = condition[3];

            int columnIndex = columns.indexOf(column);
            if (columnIndex == -1) {
                throw new IllegalArgumentException("Column not found in condition: " + column);
            }

            DataType rowValue = row[columnIndex];
            DataType compareValue = value == null ? null : DataType.fromString(value);
            boolean conditionResult = evaluateCondition(rowValue, operator, compareValue);

            if (isFirstCondition) {
                result = conditionResult;
                isFirstCondition = false;
            } else {
                if ("OR".equalsIgnoreCase(logicalOp)) {
                    result = result || conditionResult;
                } else if ("AND".equalsIgnoreCase(logicalOp)) {
                    result = result && conditionResult;
                } else {
                    throw new IllegalArgumentException("Unsupported logical operator: " + logicalOp);
                }
            }
        }

        return result;
    }

    /**
     * Evaluates a single condition against a row value.
     *
     * @param rowValue     The DataType value from the row.
     * @param operator     The comparison operator.
     * @param compareValue The DataType value to compare against.
     * @return True if the condition is satisfied, false otherwise.
     */
    private boolean evaluateCondition(DataType rowValue, String operator, DataType compareValue) {
        if (rowValue == null) {
            return false;
        }

        // Assuming DataType implements Comparable<DataType>
        switch (operator) {
            case "=":
                return rowValue.equals(compareValue);
            case "!=":
                return !rowValue.equals(compareValue);
            case ">":
                return rowValue.compareTo(compareValue) > 0;
            case "<":
                return rowValue.compareTo(compareValue) < 0;
            case ">=":
                return rowValue.compareTo(compareValue) >= 0;
            case "<=":
                return rowValue.compareTo(compareValue) <= 0;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }

    /**
     * Sorts the matched rows based on the ID column in ascending order.
     *
     * @param matchedRows List of DataType[] rows that matched the conditions.
     */
    private void sortRowsById(List<DataType[]> matchedRows) {
        int idIndex = columns.indexOf("ID");
        if (idIndex == -1) {
            throw new IllegalArgumentException("ID column not found for sorting.");
        }

        matchedRows.sort((row1, row2) -> {
            DataType id1 = row1[idIndex];
            DataType id2 = row2[idIndex];

            if (id1 == null && id2 == null) return 0;
            if (id1 == null) return -1;
            if (id2 == null) return 1;

            // Ensure both IDs are of the same type
            if (id1.getType() != id2.getType()) {
                throw new IllegalArgumentException("ID types do not match.");
            }

            switch (id1.getType()) {
                case INTEGER:
                    Integer int1 = (Integer) id1.getValue();
                    Integer int2 = (Integer) id2.getValue();
                    return int1.compareTo(int2);
                case DOUBLE:
                    Double double1 = (Double) id1.getValue();
                    Double double2 = (Double) id2.getValue();
                    return double1.compareTo(double2);
                case STRING:
                    String str1 = (String) id1.getValue();
                    String str2 = (String) id2.getValue();
                    return str1.compareTo(str2);
                default:
                    throw new IllegalArgumentException("Unsupported DataType for ID column: " + id1.getType());
            }
        });
    }
    /**
     * Determines if the queues should be rebalanced based on the current load.
     *
     * @return True if rebalancing is needed, false otherwise.
     */
    private boolean shouldRebalance() {
        if (operationCount < REBALANCE_THRESHOLD) {
            return false;
        }

        int minSize = Integer.MAX_VALUE;
        int maxSize = 0;

        for (Queue<DataType[]> queue : queues) {
            int size = queue.size();
            minSize = Math.min(minSize, size);
            maxSize = Math.max(maxSize, size);
        }

        return maxSize > minSize * SIZE_IMBALANCE_THRESHOLD;
    }

    /**
     * Rebalances the queues to ensure an even distribution of rows.
     */
    private void rebalanceQueues() {
        // Collect all rows from all queues
        List<DataType[]> allRows = new ArrayList<>();
        for (Queue<DataType[]> queue : queues) {
            allRows.addAll(queue);
            queue.clear();
        }

        // Redistribute rows across queues based on round-robin distribution
        for (DataType[] row : allRows) {
            int queueIndex = currentQueue % NUM_QUEUES;
            queues.get(queueIndex).offer(row);
            currentQueue++;
        }

        operationCount = 0; // Reset operation count after rebalancing
    }

    /**
     * Increments the operation count.
     */
    private void incrementOperationCount() {
        operationCount++;
    }

    /**
     * Validates the input values against the columns.
     *
     * @param values List of values to validate.
     */
    private void validateValues(List<String> values) {
        if (values == null) {
            throw new IllegalArgumentException("Values list cannot be null.");
        }
        if (values.size() != columns.size()) {
            throw new IllegalArgumentException("Value count mismatch. Expected "
                    + columns.size() + ", got " + values.size());
        }
    }
}
