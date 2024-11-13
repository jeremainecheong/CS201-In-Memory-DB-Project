package edu.smu.smusql.evaluation.metrics;

import java.util.*;

/**
 * Tracks performance metrics specific to different data types
 */
public class DataTypeMetrics {
    // Map of data type to operation-specific latencies
    private Map<String, Map<String, List<Long>>> dataTypeOperationLatencies = new HashMap<>();
    
    // Map of data type to memory usage
    private Map<String, Long> dataTypeMemoryUsage = new HashMap<>();
    
    /**
     * Records a latency measurement for a specific data type and operation
     * @param dataType The data type (e.g., "INTEGER", "STRING", etc.)
     * @param operation The operation type (e.g., "INSERT", "SELECT", etc.)
     * @param latency The latency in nanoseconds
     */
    public void recordLatency(String dataType, String operation, long latency) {
        dataTypeOperationLatencies
            .computeIfAbsent(dataType, k -> new HashMap<>())
            .computeIfAbsent(operation, k -> new ArrayList<>())
            .add(latency);
    }
    
    /**
     * Records memory usage for a specific data type
     * @param dataType The data type
     * @param bytes Memory used in bytes
     */
    public void recordMemoryUsage(String dataType, long bytes) {
        dataTypeMemoryUsage.merge(dataType, bytes, Long::sum);
    }
    
    /**
     * Gets average latency for a specific data type and operation
     * @param dataType The data type
     * @param operation The operation type
     * @return Average latency in nanoseconds
     */
    public double getAverageLatency(String dataType, String operation) {
        Map<String, List<Long>> operationMap = dataTypeOperationLatencies.get(dataType);
        if (operationMap == null) return 0.0;
        
        List<Long> latencies = operationMap.get(operation);
        if (latencies == null || latencies.isEmpty()) return 0.0;
        
        return latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
    }
    
    /**
     * Gets memory usage for a specific data type in MB
     * @param dataType The data type
     * @return Memory usage in MB
     */
    public double getMemoryUsageMB(String dataType) {
        Long bytes = dataTypeMemoryUsage.get(dataType);
        if (bytes == null) return 0.0;
        return bytes / (1024.0 * 1024.0);
    }
    
    /**
     * Gets all tracked data types
     * @return Set of data types being tracked
     */
    public Set<String> getDataTypes() {
        return new HashSet<>(dataTypeOperationLatencies.keySet());
    }
    
    /**
     * Gets all operations tracked for a specific data type
     * @param dataType The data type
     * @return Set of operations, or empty set if data type not found
     */
    public Set<String> getOperations(String dataType) {
        Map<String, List<Long>> operationMap = dataTypeOperationLatencies.get(dataType);
        return operationMap != null ? operationMap.keySet() : Collections.emptySet();
    }
    
    /**
     * Creates a detailed summary of data type metrics
     * @return A string containing the metrics summary
     */
    @Override
    public String toString() {
        StringBuilder summary = new StringBuilder();
        summary.append("Data Type Performance Metrics:\n");
        
        for (String dataType : getDataTypes()) {
            summary.append(String.format("\n%s:\n", dataType));
            summary.append(String.format("  Memory Usage: %.2f MB\n", getMemoryUsageMB(dataType)));
            
            for (String operation : getOperations(dataType)) {
                double avgLatencyMs = getAverageLatency(dataType, operation) / 1_000_000.0;
                summary.append(String.format("  %s Avg Latency: %.3f ms\n", operation, avgLatencyMs));
            }
        }
        
        return summary.toString();
    }
}
