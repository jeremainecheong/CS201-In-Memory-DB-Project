package edu.smu.smusql.evaluation.metrics;

import java.util.*;

public class TestResults {
    private Map<String, List<Long>> operationLatencies = new HashMap<>();
    private Map<String, Long> memoryUsage = new HashMap<>();
    private Map<String, Integer> operationCounts = new HashMap<>();
    private long totalDuration;
    private long peakMemoryUsage;
    
    public void recordLatency(String operation, long latency) {
        operationLatencies.computeIfAbsent(operation, k -> new ArrayList<>()).add(latency);
        operationCounts.merge(operation, 1, Integer::sum);
    }

    public void recordMemory(String phase, long memory) {
        memoryUsage.put(phase, memory);
        peakMemoryUsage = Math.max(peakMemoryUsage, memory);
    }

    public Map<String, List<Long>> getOperationLatencies() {
        return operationLatencies;
    }

    public Map<String, Long> getMemoryUsage() {
        return memoryUsage;
    }

    public Map<String, Integer> getOperationCounts() {
        return operationCounts;
    }

    public long getTotalDuration() {
        return totalDuration;
    }

    public void setTotalDuration(long totalDuration) {
        this.totalDuration = totalDuration;
    }

    public long getPeakMemoryUsage() {
        return peakMemoryUsage;
    }
}
