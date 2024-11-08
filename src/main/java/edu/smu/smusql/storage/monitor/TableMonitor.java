package edu.smu.smusql.storage.monitor;

import edu.smu.smusql.storage.Table;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TableMonitor implements Table {
    private final Table table;
    private final String implementationName;
    private final Stats stats = new Stats();

    // Track different types of metrics
    private static class Stats {
        // Operation counts
        AtomicLong insertCount = new AtomicLong(0);
        AtomicLong selectCount = new AtomicLong(0);
        AtomicLong updateCount = new AtomicLong(0);
        AtomicLong deleteCount = new AtomicLong(0);

        // Timing metrics (in nanoseconds)
        Map<String, List<Long>> operationTimes = new ConcurrentHashMap<>();

        // Memory tracking
        List<Long> memoryUsage = new ArrayList<>();

        // Hit/Miss tracking for data structures
        AtomicLong mainHits = new AtomicLong(0);
        AtomicLong overflowHits = new AtomicLong(0);

        // Batch operation metrics
        long lastBatchTime = System.nanoTime();
        int batchSize = 1000;
        Map<String, Double> avgBatchTimes = new ConcurrentHashMap<>();

        void recordTime(String operation, long time) {
            operationTimes.computeIfAbsent(operation, k -> new ArrayList<>()).add(time);
        }

        void recordMemory() {
            Runtime rt = Runtime.getRuntime();
            long usedMemory = rt.totalMemory() - rt.freeMemory();
            memoryUsage.add(usedMemory);
        }

        // Get aggregated stats
        Map<String, Object> getStats() {
            Map<String, Object> result = new HashMap<>();

            // Operation counts
            result.put("Total Inserts", insertCount.get());
            result.put("Total Selects", selectCount.get());
            result.put("Total Updates", updateCount.get());
            result.put("Total Deletes", deleteCount.get());

            // Average operation times
            Map<String, Double> avgTimes = new HashMap<>();
            for (Map.Entry<String, List<Long>> entry : operationTimes.entrySet()) {
                double avg = entry.getValue().stream()
                        .mapToLong(Long::longValue)
                        .average()
                        .orElse(0.0);
                avgTimes.put(entry.getKey() + " Avg Time (ms)", avg / 1_000_000.0);
            }
            result.put("Operation Times", avgTimes);

            // Memory metrics
            if (!memoryUsage.isEmpty()) {
                result.put("Avg Memory (MB)",
                        memoryUsage.stream().mapToLong(Long::longValue).average().getAsDouble() / (1024 * 1024));
                result.put("Max Memory (MB)",
                        memoryUsage.stream().mapToLong(Long::longValue).max().getAsLong() / (1024 * 1024));
            }

            // Hit ratios
            long totalHits = mainHits.get() + overflowHits.get();
            if (totalHits > 0) {
                result.put("Main Hit Ratio", (double) mainHits.get() / totalHits);
                result.put("Overflow Hit Ratio", (double) overflowHits.get() / totalHits);
            }

            // Batch performance
            result.put("Batch Times", avgBatchTimes);

            return result;
        }
    }

    public TableMonitor(Table table, String name) {
        this.table = table;
        this.implementationName = name;
    }

    @Override
    public void insert(List<String> values) {
        stats.insertCount.incrementAndGet();
        long start = System.nanoTime();

        try {
            table.insert(values);
        } finally {
            long time = System.nanoTime() - start;
            stats.recordTime("insert", time);
            stats.recordMemory();
            checkBatch("insert");
        }
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        stats.selectCount.incrementAndGet();
        long start = System.nanoTime();

        try {
            List<Map<String, String>> results = table.select(conditions);
            if (results != null && !results.isEmpty()) {
                stats.mainHits.incrementAndGet();
            } else {
                stats.overflowHits.incrementAndGet();
            }
            return results;
        } finally {
            long time = System.nanoTime() - start;
            stats.recordTime("select", time);
            stats.recordMemory();
            checkBatch("select");
        }
    }

    @Override
    public int update(String column, String value, List<String[]> conditions) {
        stats.updateCount.incrementAndGet();
        long start = System.nanoTime();

        try {
            return table.update(column, value, conditions);
        } finally {
            long time = System.nanoTime() - start;
            stats.recordTime("update", time);
            stats.recordMemory();
            checkBatch("update");
        }
    }

    @Override
    public int delete(List<String[]> conditions) {
        stats.deleteCount.incrementAndGet();
        long start = System.nanoTime();

        try {
            return table.delete(conditions);
        } finally {
            long time = System.nanoTime() - start;
            stats.recordTime("delete", time);
            stats.recordMemory();
            checkBatch("delete");
        }
    }

    @Override
    public List<String> getColumns() {
        return table.getColumns();
    }

    // Check batch performance
    private void checkBatch(String operation) {
        long totalOps = stats.insertCount.get() + stats.selectCount.get() +
                stats.updateCount.get() + stats.deleteCount.get();

        if (totalOps % stats.batchSize == 0) {
            long now = System.nanoTime();
            double batchTime = (now - stats.lastBatchTime) / 1_000_000_000.0;
            stats.avgBatchTimes.compute(operation + " Batch",
                    (k, v) -> v == null ? batchTime : (v + batchTime) / 2);
            stats.lastBatchTime = now;
        }
    }

    // Inside TableMonitor's getPerformanceReport method:
    public String getPerformanceReport() {
        Map<String, Object> statistics = stats.getStats();
        StringBuilder report = new StringBuilder();

        report.append("\n=== Performance Report for ").append(implementationName).append(" ===\n");

        // Operation Counts
        report.append("\nOperation Counts:\n");
        report.append("Inserts: ").append(statistics.get("Total Inserts")).append("\n");
        report.append("Selects: ").append(statistics.get("Total Selects")).append("\n");
        report.append("Updates: ").append(statistics.get("Total Updates")).append("\n");
        report.append("Deletes: ").append(statistics.get("Total Deletes")).append("\n");

        // Operation Times
        @SuppressWarnings("unchecked")
        Map<String, Double> times = (Map<String, Double>) statistics.get("Operation Times");
        report.append("\nAverage Operation Times (ms):\n");
        times.forEach((k, v) -> report.append(k).append(": ").append(String.format("%.3f", v)).append("\n"));

        // Memory Usage
        report.append("\nMemory Metrics:\n");
        report.append("Average Memory (MB): ").append(String.format("%.2f", (Double)statistics.get("Avg Memory (MB)"))).append("\n");
        report.append("Maximum Memory (MB): ").append(String.format("%.2f", (Double)statistics.get("Max Memory (MB)"))).append("\n");

        // Hit Ratios
        report.append("\nHit Ratios:\n");
        Double mainHitRatio = (Double)statistics.get("Main Hit Ratio");
        Double overflowHitRatio = (Double)statistics.get("Overflow Hit Ratio");
        report.append("Main Storage: ").append(String.format("%.2f%%", mainHitRatio * 100)).append("\n");
        report.append("Overflow/Secondary: ").append(String.format("%.2f%%", overflowHitRatio * 100)).append("\n");

        // Batch Performance
        @SuppressWarnings("unchecked")
        Map<String, Double> batchTimes = (Map<String, Double>) statistics.get("Batch Times");
        report.append("\nBatch Performance (seconds):\n");
        batchTimes.forEach((k, v) -> report.append(k).append(": ").append(String.format("%.3f", v)).append("\n"));

        return report.toString();
    }
}