package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.DataType;

import java.util.*;

public class LFUTable implements Table {
    private static final int CACHE_CAPACITY = 1000;
    
    private final List<String> columnNames;
    private final Map<Integer, DataType[]> cache; // Stores actual Data
    private final Map<Integer, Integer> frequencies; // Tracks frequency per key
    private final Map<Integer, LinkedHashSet<Integer>> freqList; // Groups keys by frequency
    private int minFrequency;
    private int size;
    
    // private class CacheNode {
    //     DataType[] data;
        
    //     CacheNode(DataType[] data) {
    //         this.data = data;
    //     }
    // }
    
    public LFUTable(List<String> columns) {
        this.columnNames = new ArrayList<>(columns);
        this.cache = new HashMap<>();
        this.frequencies = new HashMap<>();
        this.freqList = new HashMap<>();
        this.minFrequency = 0;
        this.size = 0;
        
        freqList.put(1, new LinkedHashSet<>());
    }
    
    private void incrementFrequency(int key) {
        int freq = frequencies.get(key);
        frequencies.put(key, freq + 1);
        freqList.get(freq).remove(key);
        
        if (freq == minFrequency && freqList.get(freq).isEmpty()) {
            minFrequency++;
        }
        
        freqList.computeIfAbsent(freq + 1, k -> new LinkedHashSet<>()).add(key);
    }
    
    private void evict() {
        LinkedHashSet<Integer> minFreqSet = freqList.get(minFrequency);
        int keyToRemove = minFreqSet.iterator().next();
        minFreqSet.remove(keyToRemove);
        cache.remove(keyToRemove);
        frequencies.remove(keyToRemove);
        size--;
    }
    
    @Override
    public void insert(List<String> values) {
        DataType[] row = createRow(values);
        if (row == null) return;
        
        int key = Arrays.hashCode(row);

        if (cache.containsKey(key)) {
            cache.put(key, row);
            incrementFrequency(key);
            return;
        }
        
        if (size >= CACHE_CAPACITY) {
            evict();
        }
        
        cache.put(key, row);
        frequencies.put(key, 1);
        freqList.get(1).add(key);
        minFrequency = 1;
        size++;
    }
    
    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();
        
        for (Map.Entry<Integer, DataType[]> entry : cache.entrySet()) {
            if (matchesConditions(entry.getValue(), conditions)) {
                incrementFrequency(entry.getKey());
                results.add(rowToMap(entry.getValue()));
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
        
        for (Map.Entry<Integer, DataType[]> entry : new HashMap<>(cache).entrySet()) {
            if (matchesConditions(entry.getValue(), conditions)) {
                entry.getValue()[columnIndex] = newDataValue;
                incrementFrequency(entry.getKey());
                updateCount++;
            }
        }
        
        return updateCount;
    }
    
    @Override
    public int delete(List<String[]> conditions) {
        int deleteCount = 0;
        Iterator<Map.Entry<Integer, DataType[]>> it = cache.entrySet().iterator();
        
        while (it.hasNext()) {
            Map.Entry<Integer, DataType[]> entry = it.next();
            if (matchesConditions(entry.getValue(), conditions)) {
                int key = entry.getKey();
                int freq = frequencies.get(key);
                freqList.get(freq).remove(key);
                frequencies.remove(key);
                it.remove();
                size--;
                deleteCount++;
            }
        }
        
        return deleteCount;
    }
    
    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columnNames);
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
}