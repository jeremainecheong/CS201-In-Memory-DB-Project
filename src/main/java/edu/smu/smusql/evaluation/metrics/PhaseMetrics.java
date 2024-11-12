package edu.smu.smusql.evaluation.metrics;

import java.util.*;

/**
 * Holds metrics for a single phase of testing
 */
public class PhaseMetrics {
    public long totalTime;            // Total time taken for the phase in nanoseconds
    public long memoryUsed;           // Memory used during the phase in bytes
    public Map<String, List<Long>> operationLatencies = new HashMap<>();  // Operation-wise latencies

    /**
     * Records the latency for a specific operation type
     * @param operation The type of operation (e.g., "INSERT", "SELECT", etc.)
     * @param latency The latency in nanoseconds
     */
    public void recordOperationLatency(String operation, long latency) {
        operationLatencies.computeIfAbsent(operation, k -> new ArrayList<>()).add(latency);
    }

    /**
     * Gets the average latency for a specific operation type
     * @param operation The operation type
     * @return The average latency in nanoseconds, or 0 if no data exists
     */
    public double getAverageLatency(String operation) {
        List<Long> latencies = operationLatencies.get(operation);
        if (latencies == null || latencies.isEmpty()) {
            return 0.0;
        }
        return latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
    }

    /**
     * Gets the total number of operations performed
     * @return Total operation count
     */
    public int getTotalOperations() {
        return operationLatencies.values().stream()
                .mapToInt(List::size)
                .sum();
    }

    /**
     * Gets the operation throughput (operations per second)
     * @return Operations per second
     */
    public double getThroughput() {
        if (totalTime == 0) return 0.0;
        return (getTotalOperations() * 1_000_000_000.0) / totalTime;
    }

    /**
     * Gets the memory usage in megabytes
     * @return Memory usage in MB
     */
    public double getMemoryUsageMB() {
        return memoryUsed / (1024.0 * 1024.0);
    }

    /**
     * Gets latency percentiles for a specific operation
     * @param operation The operation type
     * @param percentile The percentile to calculate (0-100)
     * @return The latency value at the specified percentile, or 0 if no data exists
     */
    public long getLatencyPercentile(String operation, double percentile) {
        List<Long> latencies = operationLatencies.get(operation);
        if (latencies == null || latencies.isEmpty()) {
            return 0L;
        }

        List<Long> sortedLatencies = new ArrayList<>(latencies);
        Collections.sort(sortedLatencies);

        int index = (int) Math.ceil(percentile / 100.0 * sortedLatencies.size()) - 1;
        index = Math.max(0, Math.min(index, sortedLatencies.size() - 1));

        return sortedLatencies.get(index);
    }

    /**
     * Creates a summary of the metrics
     * @return A string containing a summary of the metrics
     */
    @Override
    public String toString() {
        StringBuilder summary = new StringBuilder();
        summary.append(String.format("Total Time: %.3f seconds\n", totalTime / 1_000_000_000.0));
        summary.append(String.format("Memory Used: %.2f MB\n", getMemoryUsageMB()));
        summary.append(String.format("Total Operations: %d\n", getTotalOperations()));
        summary.append(String.format("Throughput: %.2f ops/sec\n", getThroughput()));

        summary.append("\nOperation Latencies (ms):\n");
        for (String operation : operationLatencies.keySet()) {
            summary.append(String.format("%s:\n", operation));
            summary.append(String.format("  Avg: %.3f\n", getAverageLatency(operation) / 1_000_000.0));
            summary.append(String.format("  P50: %.3f\n", getLatencyPercentile(operation, 50) / 1_000_000.0));
            summary.append(String.format("  P90: %.3f\n", getLatencyPercentile(operation, 90) / 1_000_000.0));
            summary.append(String.format("  P99: %.3f\n", getLatencyPercentile(operation, 99) / 1_000_000.0));
        }

        return summary.toString();
    }
}