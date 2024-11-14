package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.DataType;

import java.util.*;

/**
 * ChunkTable organizes data in fixed-size chunks optimized for sequential access and batch operations
 * Each chunk maintains its own ordering and handles insertions, deletions, and updates.
 */
public class ChunkTable implements Table {
    private static final int CHUNK_SIZE = 128; // Rows per chunk

    private final List<String> columnNames;
    private final List<Chunk> chunks;
    private Chunk currentChunk;

    /**
     * Represents a fixed-size block of data.
     */
    private class Chunk {
        private final DataType[][] rows;
        private final BitSet validRows;
        private int size;
        private int deletedCount;

        Chunk() {
            this.rows = new DataType[CHUNK_SIZE][];
            this.validRows = new BitSet(CHUNK_SIZE);
            this.size = 0;
            this.deletedCount = 0;
        }

        /**
         * Checks if the chunk has reached its maximum capacity.
         *
         * @return True if full, else false.
         */
        boolean isFull() {
            return size >= CHUNK_SIZE;
        }

        /**
         * Determines if the chunk should undergo compaction based on deletion count.
         *
         * @return True if compaction is needed, else false.
         */
        boolean shouldReorganize() {
            return deletedCount > CHUNK_SIZE / 3;  // Reorganize when >1/3 deleted
        }

        /**
         * Adds a new row to the chunk.
         *
         * @param row The DataType array representing the row.
         */
        void addRow(DataType[] row) {
            if (size < CHUNK_SIZE) {
                rows[size] = row;
                validRows.set(size);
                size++;
            }
        }

        /**
         * Marks a row as deleted.
         *
         * @param index The index of the row within the chunk.
         */
        void deleteRow(int index) {
            if (validRows.get(index)) {
                validRows.clear(index);
                rows[index] = null; // Help garbage collection
                deletedCount++;
            }
        }

        /**
         * Compacts the chunk by removing deleted rows.
         */
        void compact() {
            DataType[][] newRows = new DataType[CHUNK_SIZE][];
            BitSet newValidRows = new BitSet(CHUNK_SIZE);
            int newSize = 0;

            for (int i = 0; i < size; i++) {
                if (validRows.get(i)) {
                    newRows[newSize] = rows[i];
                    newValidRows.set(newSize);
                    newSize++;
                }
            }

            // Reset the chunk
            for (int i = 0; i < CHUNK_SIZE; i++) {
                rows[i] = null;
            }
            validRows.clear();
            size = newSize;
            deletedCount = 0;

            // Populate with compacted data
            for (int i = 0; i < newSize; i++) {
                rows[i] = newRows[i];
                validRows.set(i);
            }
        }
    }

    /**
     * Constructs a ChunkTable with the specified columns.
     *
     * @param columns List of column names.
     */
    public ChunkTable(List<String> columns) {
        this.columnNames = new ArrayList<>(columns);
        this.chunks = new ArrayList<>();
        this.currentChunk = new Chunk();
        this.chunks.add(currentChunk);
    }

    @Override
    public void insert(List<String> values) {
        DataType[] row = createRow(values);
        if (row == null) return;

        if (currentChunk.isFull()) {
            // Start a new chunk
            currentChunk = new Chunk();
            chunks.add(currentChunk);
        }

        currentChunk.addRow(row);
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();

        for (Chunk chunk : chunks) {
            // Process each chunk
            for (int i = 0; i < chunk.size; i++) {
                if (chunk.validRows.get(i)) {
                    DataType[] row = chunk.rows[i];
                    if (matchesConditions(row, conditions)) {
                        Map<String, String> resultRow = rowToMap(row);
                        results.add(resultRow);
                    }
                }
            }
        }

        return results;
    }

    @Override
    public int update(String column, String newValue, List<String[]> conditions) {
        int updateCount = 0;
        DataType newDataValue = DataType.fromString(newValue);
        int columnIndex = columnNames.indexOf(column);

        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        for (Chunk chunk : chunks) {
            for (int i = 0; i < chunk.size; i++) {
                if (chunk.validRows.get(i)) {
                    DataType[] row = chunk.rows[i];
                    if (matchesConditions(row, conditions)) {
                        row[columnIndex] = newDataValue;
                        updateCount++;
                    }
                }
            }
        }

        return updateCount;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int deleteCount = 0;

        for (Chunk chunk : chunks) {
            for (int i = 0; i < chunk.size; i++) {
                if (chunk.validRows.get(i) && matchesConditions(chunk.rows[i], conditions)) {
                    chunk.deleteRow(i);
                    deleteCount++;
                }
            }

            if (chunk.shouldReorganize()) {
                chunk.compact();
            }
        }

        return deleteCount;
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columnNames);
    }

    // Helper methods for row operations

    /**
     * Creates a DataType array from the list of string values.
     *
     * @param values The list of string values.
     * @return DataType array representing the row.
     */
    private DataType[] createRow(List<String> values) {
        if (values.size() != columnNames.size()) {
            throw new IllegalArgumentException("Value count mismatch");
        }

        DataType[] row = new DataType[columnNames.size()];
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) != null && !values.get(i).equalsIgnoreCase("null")) {
                // Trim and sanitize input to remove unwanted characters
                String sanitizedValue = values.get(i).trim().replaceAll("[)]", ""); // Removes trailing )
                row[i] = DataType.fromString(sanitizedValue);
            } else {
                row[i] = null; // Keep null if the value is "null" or empty
            }
        }
        return row;
    }

    /**
     * Evaluates if a row matches the provided conditions.
     *
     * @param row        The row data.
     * @param conditions The list of conditions.
     * @return True if the row matches all conditions, else false.
     */
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
     *
     * @param row      The row data.
     * @param column   The column name.
     * @param operator The operator (e.g., =, >, <).
     * @param value    The value to compare.
     * @return True if the condition is satisfied, else false.
     */
    private boolean evaluateCondition(DataType[] row, String column, String operator, String value) {
        int colIndex = columnNames.indexOf(column);
        if (colIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        DataType rowValue = row[colIndex];
        if (rowValue == null) return false;

        DataType compareValue = DataType.fromString(value);

        switch (operator) {
            case "=":
                return rowValue.equals(compareValue);
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
     * Converts a row's DataType array into a Map for easy representation.
     *
     * @param row The row data.
     * @return Map with column names as keys and their corresponding string values.
     */
    private Map<String, String> rowToMap(DataType[] row) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            if (row[i] != null) {
                map.put(columnNames.get(i), row[i].toString());
            } else {
                map.put(columnNames.get(i), "null");
            }
        }
        return map;
    }
}
