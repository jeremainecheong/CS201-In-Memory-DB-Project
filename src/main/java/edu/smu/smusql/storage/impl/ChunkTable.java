package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.DataType;

import java.util.*;

public class ChunkTable implements Table {
    private static final int CHUNK_SIZE = 128;

    private final List<String> columnNames;
    private final List<Chunk> chunks;
    private final Map<DataType, ChunkLocation> primaryKeyIndex;
    private Chunk currentChunk;
    private final String idColumn;

    private static class ChunkLocation {
        final Chunk chunk;
        final int index;

        ChunkLocation(Chunk chunk, int index) {
            this.chunk = chunk;
            this.index = index;
        }
    }

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

        boolean isFull() {
            return size >= CHUNK_SIZE;
        }

        boolean shouldReorganize() {
            return deletedCount > CHUNK_SIZE / 3;
        }

        void addRow(DataType[] row) {
            if (size < CHUNK_SIZE) {
                rows[size] = row;
                validRows.set(size);
                size++;
            }
        }

        void deleteRow(int index) {
            if (validRows.get(index)) {
                validRows.clear(index);
                rows[index] = null;
                deletedCount++;
            }
        }

        Map<DataType, Integer> compact() {
            Map<DataType, Integer> newPositions = new HashMap<>();
            DataType[][] newRows = new DataType[CHUNK_SIZE][];
            BitSet newValidRows = new BitSet(CHUNK_SIZE);
            int newSize = 0;

            for (int i = 0; i < size; i++) {
                if (validRows.get(i)) {
                    newRows[newSize] = rows[i];
                    newValidRows.set(newSize);
                    newPositions.put(rows[i][0], newSize); // Store new position using primary key
                    newSize++;
                }
            }

            // Reset and update chunk
            Arrays.fill(rows, null);
            validRows.clear();
            size = newSize;
            deletedCount = 0;

            // Copy compacted data back
            System.arraycopy(newRows, 0, rows, 0, newSize);
            validRows.or(newValidRows);

            return newPositions;
        }
    }

    public ChunkTable(List<String> columns) {
        this.columnNames = new ArrayList<>(columns);
        this.chunks = new ArrayList<>();
        this.primaryKeyIndex = new HashMap<>();
        this.currentChunk = new Chunk();
        this.chunks.add(currentChunk);
        this.idColumn = columns.get(0);
    }

    @Override
    public void insert(List<String> values) {
        DataType[] row = createRow(values);
        if (row == null) {
            return;
        }

        DataType primaryKey = row[0];

        // Delete any existing entry first
        ChunkLocation existingLoc = primaryKeyIndex.get(primaryKey);
        if (existingLoc != null) {
            existingLoc.chunk.deleteRow(existingLoc.index);
            if (existingLoc.chunk.shouldReorganize()) {
                reorganizeChunk(existingLoc.chunk);
            }
        }
        primaryKeyIndex.remove(primaryKey);

        // Create new chunk if current is full
        if (currentChunk.isFull()) {
            currentChunk = new Chunk();
            chunks.add(currentChunk);
        }

        int insertIndex = currentChunk.size;
        currentChunk.addRow(row);
        primaryKeyIndex.put(primaryKey, new ChunkLocation(currentChunk, insertIndex));
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();
        DataType targetId = extractIdFromConditions(conditions);

        if (targetId != null) {
            ChunkLocation location = primaryKeyIndex.get(targetId);

            if (location != null) {
                if (location.chunk.validRows.get(location.index)) {
                    DataType[] row = location.chunk.rows[location.index];
                    if (matchesConditions(row, conditions)) {
                        results.add(rowToMap(row));
                    }
                } else {
                    primaryKeyIndex.remove(targetId);
                }
            } else {
//                System.err.println("No index entry found for ID: " + targetId);
            }
        } else {
            // Full scan
            Set<DataType> seenIds = new HashSet<>();
            int totalRows = chunks.size() * CHUNK_SIZE;  // Total possible rows
            int validRows = 0;  // Track actual valid rows

            for (Chunk chunk : chunks) {
                for (int i = 0; i < chunk.size; i++) {
                    if (chunk.validRows.get(i)) {
                        validRows++;
                        DataType[] row = chunk.rows[i];
                        if (!seenIds.contains(row[0]) && matchesConditions(row, conditions)) {
                            results.add(rowToMap(row));
                            seenIds.add(row[0]);
                            // Update index
                            primaryKeyIndex.put(row[0], new ChunkLocation(chunk, i));
                        }
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

        DataType targetId = extractIdFromConditions(conditions);

        if (targetId != null) {
            // Primary key update
            ChunkLocation location = primaryKeyIndex.get(targetId);
            if (location != null && location.chunk.validRows.get(location.index)) {
                DataType[] row = location.chunk.rows[location.index];
                if (matchesConditions(row, conditions)) {
                    row[columnIndex] = newDataValue;
                    updateCount++;
                }
            }
        } else {
            // Full scan update
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
        }

        return updateCount;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int deleteCount = 0;
        DataType targetId = extractIdFromConditions(conditions);

        if (targetId != null) {
            ChunkLocation location = primaryKeyIndex.get(targetId);
            if (location != null && location.chunk.validRows.get(location.index)) {
                DataType[] row = location.chunk.rows[location.index];
                if (matchesConditions(row, conditions)) {
                    location.chunk.deleteRow(location.index);
                    primaryKeyIndex.remove(targetId);
                    deleteCount++;

                    if (location.chunk.shouldReorganize()) {
                        reorganizeChunk(location.chunk);
                    }
                }
            }
        } else {
            for (Chunk chunk : chunks) {
                boolean chunkModified = false;
                for (int i = 0; i < chunk.size; i++) {
                    if (chunk.validRows.get(i) && matchesConditions(chunk.rows[i], conditions)) {
                        DataType rowId = chunk.rows[i][0];
                        primaryKeyIndex.remove(rowId);
                        chunk.deleteRow(i);
                        deleteCount++;
                        chunkModified = true;
                    }
                }
                if (chunkModified && chunk.shouldReorganize()) {
                    reorganizeChunk(chunk);
                }
            }
        }

        return deleteCount;
    }

    private void reorganizeChunk(Chunk chunk) {
        Map<DataType, Integer> newPositions = chunk.compact();

        primaryKeyIndex.entrySet().removeIf(entry -> {
            ChunkLocation loc = entry.getValue();
            if (loc.chunk == chunk) {
                Integer newPos = newPositions.get(entry.getKey());
                if (newPos != null) {
                    entry.setValue(new ChunkLocation(chunk, newPos));
                    return false;
                }
                return true;
            }
            return false;
        });
    }

    private DataType[] createRow(List<String> values) {
        if (values.size() != columnNames.size()) {
            throw new IllegalArgumentException("Value count mismatch");
        }

        DataType[] row = new DataType[columnNames.size()];
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            row[i] = (value != null && !value.equalsIgnoreCase("null"))
                    ? DataType.fromString(value.trim().replaceAll("[)]", ""))
                    : null;
        }
        return row;
    }

    private DataType extractIdFromConditions(List<String[]> conditions) {
        if (conditions == null || conditions.isEmpty()) return null;

        for (String[] condition : conditions) {
            if (condition[1].equals(idColumn) && condition[2].equals("=")) {
                return DataType.fromString(condition[3]);
            }
        }
        return null;
    }

    private boolean matchesConditions(DataType[] row, List<String[]> conditions) {
        if (conditions == null || conditions.isEmpty()) return true;

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
            boolean conditionResult = evaluateCondition(row, condition[1], condition[2], condition[3]);

            if (logicalOp.equals("OR")) {
                finalResult = finalResult || currentAndGroup;
                currentAndGroup = conditionResult;
            } else {
                currentAndGroup = currentAndGroup && conditionResult;
            }
        }

        return finalResult || currentAndGroup;
    }

    private boolean handleOrOnlyConditions(DataType[] row, List<String[]> conditions) {
        for (String[] condition : conditions) {
            if (!condition[0].equals("OR")) {
                throw new IllegalArgumentException("Mixed AND/OR not allowed in this context");
            }
            if (evaluateCondition(row, condition[1], condition[2], condition[3])) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateCondition(DataType[] row, String column, String operator, String value) {
        int colIndex = columnNames.indexOf(column);
        if (colIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        DataType rowValue = row[colIndex];
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

    private Map<String, String> rowToMap(DataType[] row) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(columnNames.get(i), row[i] != null ? row[i].toString() : "null");
        }
        return map;
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columnNames);
    }
}