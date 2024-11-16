package edu.smu.smusql.evaluation;

import edu.smu.smusql.Engine;
import edu.smu.smusql.evaluation.metrics.DataType;
import edu.smu.smusql.evaluation.metrics.TestResults;
import edu.smu.smusql.evaluation.generators.ValueGenerator;
import edu.smu.smusql.evaluation.operations.StandardQueryExecutor;
import edu.smu.smusql.evaluation.operations.ComplexQueryExecutor;
import edu.smu.smusql.evaluation.utils.ZipfianGenerator;

import java.io.IOException;
import java.util.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.MemoryPoolMXBean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Evaluator {
    private final Engine dbEngine;
    private final Random random;
    private final int NUMBER_OF_RUNS = 3;
    private final int TEST_OPERATIONS = 10000;
    private final MemoryMXBean memoryBean;
    private final Map<String, Map<DataType, List<TestResults>>> results;
    private final ValueGenerator valueGenerator;
    private final StandardQueryExecutor standardExecutor;
    private final ComplexQueryExecutor complexExecutor;
    private final List<String[]> scalabilityResults = new ArrayList<>();
    private final ZipfianGenerator zipfianGenerator = new ZipfianGenerator(1.2, 1000); // Higher alpha for skew

    public Evaluator(Engine engine) {
        this.dbEngine = engine;
        this.random = new Random();
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.results = new ConcurrentHashMap<>();
        this.valueGenerator = new ValueGenerator();
        this.standardExecutor = new StandardQueryExecutor();
        this.complexExecutor = new ComplexQueryExecutor();
    }

    public void runEvaluation(Scanner scanner) {
        // String[] implementations = { "backwards_", "chunk_", "forest_", "random_",
        // "lfu_" };
        String[] implementations = {"chunk_", "forest_", "lfu_"};
        System.out.println("\nSkip to scalability tests? (y/n)");
        System.out.flush();
        String r1 = scanner.nextLine().trim().toLowerCase();
        if (r1.equals("n") || r1.equals("no")) {
             System.out.println("Starting Comprehensive Performance Evaluation");
            for (String impl : implementations) {
                System.out.println("\nEvaluating implementation: " + impl);
                results.put(impl, new EnumMap<>(DataType.class));

                for (DataType type : DataType.values()) {
                    evaluateImplementationWithDataType(impl, type);
                }
            }
            generateReport();
        } else {
            System.out.println("Skipping to scalability tests");
        }

        System.out.println("\n--- Scalability Tests ---");
        System.out.println("\nScalability tests might take some time. Do you want to run them? (y/n)");
        System.out.flush();
        String r2 = scanner.nextLine().trim().toLowerCase();

        if (r2.equals("y") || r2.equals("yes")) {
            testScalability();
        } else {
            System.out.println("Skipping scalability tests");
        }
    }

    private void evaluateImplementationWithDataType(String implementation, DataType dataType) {
        System.out.println("\nTesting " + implementation + " with " + dataType);
        List<TestResults> typeResults = new ArrayList<>();

        for (int run = 0; run < NUMBER_OF_RUNS; run++) {
            System.out.println("Run " + (run + 1) + "/" + NUMBER_OF_RUNS);

            // Reset state and warm up
            dbEngine.reset();
            System.gc();

            TestResults runResults = executeTestRun(implementation, dataType);
            typeResults.add(runResults);
        }

        results.get(implementation).put(dataType, typeResults);
    }

    private TestResults executeTestRun(String implementation, DataType dataType) {
        TestResults results = new TestResults();
        String tableName = implementation + dataType.name().toLowerCase() + "_test";

        // Initial table creation and population
        long startTime = System.nanoTime();
        MemoryUsage beforeMem = memoryBean.getHeapMemoryUsage();

        createAndPopulateTable(tableName, dataType, results);

        results.recordMemory("population",
                memoryBean.getHeapMemoryUsage().getUsed() - beforeMem.getUsed());

        executeFrequencyTest(tableName, dataType, results);
        executeSequentialTest(tableName, dataType, results);
        executeRangeTest(tableName, dataType, results);
        // Mixed operations phase
        executeMixedOperations(tableName, dataType, results);

        // Complex queries phase
        executeComplexQueries(tableName, dataType, results);
        evaluateConditionalQueries(tableName, dataType, results);

        results.setTotalDuration(System.nanoTime() - startTime);
        return results;
    }

    private void createAndPopulateTable(String tableName, DataType dataType, TestResults results) {
        dbEngine.executeSQL("CREATE TABLE " + tableName + " (id, value)");

        for (int i = 0; i < 1000; i++) {
            String value = valueGenerator.generateValue(dataType, i);
            String query = String.format("INSERT INTO %s VALUES (%d, %s)",
                    tableName, i, value);

            long start = System.nanoTime();
            dbEngine.executeSQL(query);
            results.recordLatency("INSERT", System.nanoTime() - start);
        }
    }

    private void executeMixedOperations(String tableName, DataType dataType, TestResults results) {
        for (int i = 0; i < TEST_OPERATIONS; i++) {
            standardExecutor.executeQuery(tableName, dataType, dbEngine, results);
        }
    }

    private void executeComplexQueries(String tableName, DataType dataType, TestResults results) {
        for (int i = 0; i < 100; i++) {
            complexExecutor.executeQuery(tableName, dataType, dbEngine, results);
        }
    }

    public void testScalability() {
        System.out.println("\nStarting scalability test");
        int[] rowCounts = { 100, 1000};
        String[] implementations = { "chunk_", "lfu_" };

        for (String impl : implementations) {
            System.out.println("\nEvaluating scalability for: " + impl);

            for (int rowCount : rowCounts) {
                System.out.println("\nTesting " + impl + " with " + rowCount + " rows");

                dbEngine.reset();
                System.gc(); // Optional: trigger GC to reduce "before" memory noise

                // Record initial memory usage and start time
                long startTime = System.nanoTime();
                MemoryUsage initialMemoryUsage = memoryBean.getHeapMemoryUsage();

                // Populate table with the specified number of rows
                String tableName = impl + "scalability_test";
                populateTableForScalability(tableName, rowCount);

                // Record memory usage after population phase
                MemoryUsage postPopulationMemoryUsage = memoryBean.getHeapMemoryUsage();
                double memoryOverhead = (postPopulationMemoryUsage.getUsed() - initialMemoryUsage.getUsed())
                        / (1024.0 * 1024.0); // Convert to MB
                double timeTakenSeconds = (System.nanoTime() - startTime) / 1_000_000.0;

                System.out.printf("=== Population Phase ===\n");
                System.out.printf("Rows: %d, Time Taken: %.3f ms, Memory used: %.2f MB\n", rowCount, timeTakenSeconds,
                        memoryOverhead);

                // Run mixed operations and measure performance metrics
                System.out.println("=== Mixed Operations Phase ===");
                testOperationsForScale(tableName, rowCount, impl, memoryOverhead);
            }
        }

        // Export results to CSV
        exportScalabilityResultsToCSV("evaluation_results/scalability_test_results.csv");
    }

    private void populateTableForScalability(String tableName, int rowCount) {
        dbEngine.executeSQL("CREATE TABLE " + tableName + " (id, value)");
        for (int i = 0; i < rowCount; i++) {
            String value = valueGenerator.generateValue(DataType.SMALL_INTEGER, i);
            String query = String.format("INSERT INTO %s VALUES (%d, %s)", tableName, i, value);
            dbEngine.executeSQL(query);
        }
    }

    // Method to test scalability, print table, and store results for CSV export
    private void testOperationsForScale(String tableName, int rowCount, String implementation,
            double initialMemoryOverhead) {
        long totalLatency = 0; // Sum of all latencies
        long maxLatency = Long.MIN_VALUE; // Maximum latency
        long minLatency = Long.MAX_VALUE; // Minimum latency
        int count = 0;

        TestResults results = new TestResults();
        List<Long> latencySamples = new ArrayList<>();

        // Measure memory at start of mixed operations phase
        MemoryUsage beforeMixedOpsMemory = memoryBean.getHeapMemoryUsage();
        long initialMemoryUsed = beforeMixedOpsMemory.getUsed();

        // Execute operations and calculate metrics dynamically
        for (int i = 0; i < TEST_OPERATIONS; i++) {
            long start = System.nanoTime();
            standardExecutor.executeQuery(tableName, DataType.SMALL_INTEGER, dbEngine, results);
            long duration = System.nanoTime() - start;

            totalLatency += duration;
            maxLatency = Math.max(maxLatency, duration);
            minLatency = Math.min(minLatency, duration);

            if (i % Math.max(1, rowCount / 1000) == 0) { // Sample dynamically based on rowCount
                latencySamples.add(duration);
            }
            count++;
        }

        // Measure memory after mixed operations phase
        MemoryUsage afterMixedOpsMemory = memoryBean.getHeapMemoryUsage();
        double memoryOverhead = (afterMixedOpsMemory.getUsed() - initialMemoryUsed) / (1024.0 * 1024.0); // Convert to
                                                                                                         // MB

        // If memory overhead is negative, fall back to initial memory overhead as an
        // approximation
        if (memoryOverhead < 0) {
            memoryOverhead = initialMemoryOverhead;
        }

        long avgLatency = totalLatency / count;
        long percentile50 = (long) calculatePercentile(latencySamples, 50);
        long percentile90 = (long) calculatePercentile(latencySamples, 90);

        // Record results in TestResults for later analysis
        results.recordLatency("SCALE_TEST_AVG_" + implementation + "_" + rowCount, avgLatency);
        results.recordLatency("SCALE_TEST_50_PERCENTILE_" + implementation + "_" + rowCount, percentile50);
        results.recordLatency("SCALE_TEST_90_PERCENTILE_" + implementation + "_" + rowCount, percentile90);
        results.recordLatency("SCALE_TEST_MAX_" + implementation + "_" + rowCount, maxLatency);
        results.recordLatency("SCALE_TEST_MIN_" + implementation + "_" + rowCount, minLatency);

        // Print results in a table format
        printTable(implementation, rowCount, totalLatency, avgLatency, minLatency, maxLatency, percentile50,
                percentile90, memoryOverhead, count);

        // Add results to the scalabilityResults list for CSV export
        scalabilityResults.add(new String[] {
                implementation,
                String.valueOf(rowCount),
                String.format("%.3f", totalLatency / 1_000_000.0),
                String.format("%.3f", avgLatency / 1_000_000.0),
                String.format("%.3f", minLatency / 1_000_000.0),
                String.format("%.3f", maxLatency / 1_000_000.0),
                String.format("%.3f", percentile50 / 1_000_000.0),
                String.format("%.3f", percentile90 / 1_000_000.0),
                String.format("%.2f", memoryOverhead) // Add memory overhead in MB
        });
    }

    private double calculatePercentile(List<Long> samples, double percentile) {
        if (samples.isEmpty()) {
            return 0; // Avoid division by zero
        }

        Collections.sort(samples); // Sort samples
        int index = (int) Math.ceil(percentile / 100.0 * samples.size()) - 1;
        return samples.get(Math.max(index, 0)); // Ensure index is within bounds
    }

    private void printTable(String implementation, int rowCount, long totalLatency, long avgLatency,
            long minLatency, long maxLatency, long percentile50, long percentile90, double memoryOverhead, int count) {
        System.out.println("\n+--------------------------------------------------+");
        System.out.printf("| %-48s |\n", "Scalability Test Results");
        System.out.println("+--------------------------------------------------+");
        System.out.printf("| %-18s : %-27s |\n", "Implementation", implementation);
        System.out.printf("| %-18s : %-27d |\n", "Rows Tested", rowCount);
        System.out.printf("| %-18s : %-27.2f MB |\n", "Memory Overhead", memoryOverhead);
        System.out.println("+--------------------------------------------------+");
        System.out.printf("| %-20s | %-15s |\n", "Metric", "Value (ms)");
        System.out.println("+----------------------+-----------------+");
        System.out.printf("| %-20s | %13.3f ms |\n", "Total Time", totalLatency / 1_000_000.0);
        System.out.printf("| %-20s | %13.3f ms |\n", "Avg Time/Op", avgLatency / 1_000_000.0);
        System.out.printf("| %-20s | %13.3f ms |\n", "Min Latency", minLatency / 1_000_000.0);
        System.out.printf("| %-20s | %13.3f ms |\n", "Max Latency", maxLatency / 1_000_000.0);
        System.out.printf("| %-20s | %13.3f ms |\n", "50th Percentile", percentile50 / 1_000_000.0);
        System.out.printf("| %-20s | %13.3f ms |\n", "90th Percentile", percentile90 / 1_000_000.0);
        System.out.println("+----------------------+-----------------+");
    }

    // Method to export results to CSV
    public void exportScalabilityResultsToCSV(String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            // Write header
            writer.write(
                    "Implementation,RowCount,TotalTime(ms),AvgTimePerOp(ms),MinLatency(ms),MaxLatency(ms),50thPercentile(ms),90thPercentile(ms),MemoryOverhead(MB)");
            writer.newLine();

            // Write each row of results
            for (String[] row : scalabilityResults) {
                writer.write(String.join(",", row));
                writer.newLine();
            }

            System.out.println("Results exported to " + filePath);
        } catch (IOException e) {
            System.err.println("Failed to write results to CSV: " + e.getMessage());
        }
    }

    private void generateReport() {
        // Console output first
        System.out.println("\nDetailed Performance Report");
        System.out.println("========================");

        results.forEach((impl, typeResults) -> {
            System.out.println("\nImplementation: " + impl);
            typeResults.forEach((type, runResults) -> {
                System.out.printf("\nData Type: %s\n", type);
                printTypeStatistics(runResults);
            });
        });

        System.out.println("\n\nCOMPREHENSIVE PERFORMANCE SUMMARY");
        System.out.println("================================");
        generateComprehensiveSummary();

        // Generate CSV reports with proper exception handling
        try {
            String baseDir = "evaluation_results";
            generateCSVReports();
            generateConditionalMetricsCSV(baseDir); // Generates conditional metrics CSV
            generateAccessPatternMetricsCSV(baseDir); // Generates access pattern metrics CSVs
        } catch (IOException e) {
            System.err.println("Failed to generate CSV reports: " + e.getMessage());
        }
    }

    private void generateCSVReports() throws IOException {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String baseDir = "evaluation_results";

        // Create directory if it doesn't exist
        Path dirPath = Paths.get(baseDir);
        Files.createDirectories(dirPath);

        // Generate each report
        generateSummaryStatisticsCSV(baseDir);
        System.out.println("\nCSV report generated in directory: " + baseDir);
    }

    private void generateSummaryStatisticsCSV(String baseDir) throws IOException {
        Path filePath = Paths.get(baseDir, "summary_statistics.csv");

        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            // Write header
            writer.write("Category,Metric,Best_Implementation,Average_Time_ms,Sample_Count");
            writer.newLine();

            // Process and write implementation statistics
            processImplementationStatistics(writer);

            // Write summary sections
            writeSummarySection(writer);
            writeMemoryUsageSummary(writer);
        }
    }

    private void processImplementationStatistics(BufferedWriter writer) throws IOException {
        results.forEach((impl, typeResults) -> {
            typeResults.forEach((dataType, runResults) -> {
                String implDataType = impl + "_" + dataType;

                // Process operation latencies
                runResults.forEach(run -> {
                    run.getOperationLatencies().forEach((operation, latencies) -> {
                        double avgLatency = latencies.stream()
                                .mapToDouble(l -> l / 1_000_000.0)
                                .average()
                                .orElse(0.0);

                        try {
                            writer.write(String.format("Operation,%s,%s,%.3f,%d",
                                    operation,
                                    implDataType,
                                    avgLatency,
                                    latencies.size()));
                            writer.newLine();
                        } catch (IOException e) {
                            throw new RuntimeException("Error writing operation data", e);
                        }
                    });
                });

                // Process memory usage
                DoubleSummaryStatistics memStats = runResults.stream()
                        .mapToDouble(r -> r.getPeakMemoryUsage() / (1024.0 * 1024.0))
                        .summaryStatistics();

                try {
                    writer.write(String.format("Memory,%s,%s,%.2f,%d",
                            "Peak_MB",
                            implDataType,
                            memStats.getMax(),
                            runResults.size()));
                    writer.newLine();

                    writer.write(String.format("Memory,%s,%s,%.2f,%d",
                            "Average_MB",
                            implDataType,
                            memStats.getAverage(),
                            runResults.size()));
                    writer.newLine();
                } catch (IOException e) {
                    throw new RuntimeException("Error writing memory data", e);
                }
            });
        });
    }

    private void writeSummarySection(BufferedWriter writer) throws IOException {
        writer.newLine();
        writer.write("SUMMARY");
        writer.newLine();
        writer.write("Operation,Best_Implementation,Average_Time_ms");
        writer.newLine();

        // Calculate best performer for each operation
        Set<String> operations = new HashSet<>();
        results.values().forEach(typeResults -> typeResults.values().forEach(
                runResults -> runResults.forEach(run -> operations.addAll(run.getOperationLatencies().keySet()))));

        for (String operation : operations) {
            Map<String, Double> implPerformance = new HashMap<>();

            results.forEach((impl, typeResults) -> {
                typeResults.forEach((dataType, runResults) -> {
                    String implDataType = impl + "_" + dataType;
                    double avgLatency = runResults.stream()
                            .flatMap(run -> run.getOperationLatencies().getOrDefault(operation, new ArrayList<>())
                                    .stream())
                            .mapToDouble(l -> l / 1_000_000.0)
                            .average()
                            .orElse(Double.MAX_VALUE);
                    implPerformance.put(implDataType, avgLatency);
                });
            });

            Map.Entry<String, Double> best = implPerformance.entrySet().stream()
                    .min(Map.Entry.comparingByValue())
                    .orElse(null);

            if (best != null) {
                writer.write(String.format("%s,%s,%.3f",
                        operation,
                        best.getKey(),
                        best.getValue()));
                writer.newLine();
            }
        }
    }

    private void writeMemoryUsageSummary(BufferedWriter writer) throws IOException {
        writer.newLine();
        writer.write("MEMORY USAGE SUMMARY");
        writer.newLine();
        writer.write("Implementation_DataType,Average_MB,Peak_MB");
        writer.newLine();

        results.forEach((impl, typeResults) -> {
            typeResults.forEach((dataType, runResults) -> {
                DoubleSummaryStatistics memStats = runResults.stream()
                        .mapToDouble(r -> r.getPeakMemoryUsage() / (1024.0 * 1024.0))
                        .summaryStatistics();

                try {
                    writer.write(String.format("%s_%s,%.2f,%.2f",
                            impl,
                            dataType,
                            memStats.getAverage(),
                            memStats.getMax()));
                    writer.newLine();
                } catch (IOException e) {
                    throw new RuntimeException("Error writing memory summary", e);
                }
            });
        });
    }

    private void generateComprehensiveSummary() {
        // Structure: impl -> datatype -> operation -> stats
        Map<String, Map<String, DoubleSummaryStatistics>> operationStats = new HashMap<>();
        Map<String, Map<String, DoubleSummaryStatistics>> memoryStats = new HashMap<>();

        // Collect all operation types for consistent reporting
        Set<String> allOperations = new HashSet<>(Arrays.asList(
                "INSERT", "SELECT", "UPDATE", "DELETE",
                "COMPLEX_SELECT", "COMPLEX_UPDATE", "COMPLEX_DELETE"));

        // Aggregate statistics across all runs
        results.forEach((impl, typeResults) -> {
            typeResults.forEach((dataType, runResults) -> {
                runResults.forEach(run -> {
                    run.getOperationLatencies().forEach((op, latencies) -> {
                        String key = impl + "_" + dataType;
                        operationStats.computeIfAbsent(op, k -> new HashMap<>())
                                .computeIfAbsent(key, k -> new DoubleSummaryStatistics())
                                .accept(latencies.stream()
                                        .mapToDouble(l -> l / 1_000_000.0)
                                        .average()
                                        .orElse(0.0));
                    });

                    String key = impl + "_" + dataType;
                    memoryStats.computeIfAbsent("MEMORY", k -> new HashMap<>())
                            .computeIfAbsent(key, k -> new DoubleSummaryStatistics())
                            .accept(run.getPeakMemoryUsage() / (1024.0 * 1024.0));
                });
            });
        });

        // Print Operation Performance Comparison
        System.out.println("\nOperation Performance (Average Latency in ms)");
        System.out.println("-------------------------------------------");
        printComparisonTable(operationStats, allOperations);

        // Print Memory Usage Comparison
        System.out.println("\nMemory Usage (MB)");
        System.out.println("----------------");
        printMemoryComparison(memoryStats);

        // Print Best Performers
        System.out.println("\nBest Performers");
        System.out.println("--------------");
        printBestPerformers(operationStats, memoryStats);
    }

    private void printComparisonTable(
            Map<String, Map<String, DoubleSummaryStatistics>> stats,
            Set<String> operations) {

        // Get all implementation-datatype combinations
        Set<String> implementations = new TreeSet<>(
                stats.values().stream()
                        .flatMap(m -> m.keySet().stream())
                        .collect(Collectors.toSet()));

        // Print header
        System.out.printf("%-30s", "Implementation_DataType");
        operations.forEach(op -> System.out.printf(" | %-15s", op));
        System.out.println();
        System.out.println("-".repeat(30 + operations.size() * 18));

        // Print data rows
        implementations.forEach(impl -> {
            System.out.printf("%-30s", impl);
            operations.forEach(op -> {
                DoubleSummaryStatistics opStats = stats.getOrDefault(op, new HashMap<>())
                        .getOrDefault(impl, new DoubleSummaryStatistics());
                System.out.printf(" | %13.3f", opStats.getAverage());
            });
            System.out.println();
        });
    }

    private void printMemoryComparison(Map<String, Map<String, DoubleSummaryStatistics>> memoryStats) {
        memoryStats.get("MEMORY").forEach((impl, stats) -> {
            System.out.printf("%-30s: Avg: %.2f MB, Peak: %.2f MB\n",
                    impl, stats.getAverage(), stats.getMax());
        });
    }

    private void printBestPerformers(
            Map<String, Map<String, DoubleSummaryStatistics>> operationStats,
            Map<String, Map<String, DoubleSummaryStatistics>> memoryStats) {

        // Best for each operation type
        operationStats.forEach((op, implStats) -> {
            String bestImpl = implStats.entrySet().stream()
                    .min(Map.Entry.comparingByValue(
                            Comparator.comparingDouble(DoubleSummaryStatistics::getAverage)))
                    .map(Map.Entry::getKey)
                    .orElse("N/A");

            System.out.printf("Best for %-15s: %s (%.3f ms)\n",
                    op, bestImpl, implStats.get(bestImpl).getAverage());
        });

        // Best memory usage
        String bestMemory = memoryStats.get("MEMORY").entrySet().stream()
                .min(Map.Entry.comparingByValue(
                        Comparator.comparingDouble(DoubleSummaryStatistics::getAverage)))
                .map(Map.Entry::getKey)
                .orElse("N/A");

        System.out.printf("Best Memory Usage      : %s (%.2f MB)\n",
                bestMemory,
                memoryStats.get("MEMORY").get(bestMemory).getAverage());

        // Overall recommendations
        System.out.println("\nRecommendations:");
        System.out.println("---------------");
        generateRecommendations(operationStats, memoryStats);
    }

    private void generateRecommendations(
            Map<String, Map<String, DoubleSummaryStatistics>> operationStats,
            Map<String, Map<String, DoubleSummaryStatistics>> memoryStats) {

        // Analyze patterns and generate recommendations
        Map<String, List<String>> implStrengths = new HashMap<>();

        // Analyze each implementation's strengths
        operationStats.forEach((op, implStats) -> {
            implStats.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(
                            Comparator.comparingDouble(DoubleSummaryStatistics::getAverage)))
                    .limit(3) // Top 3 performers
                    .forEach(entry -> {
                        String impl = entry.getKey().split("_")[0]; // Get implementation name
                        implStrengths.computeIfAbsent(impl, k -> new ArrayList<>())
                                .add(op);
                    });
        });

        // Print recommendations based on use case
        implStrengths.forEach((impl, strengths) -> {
            System.out.printf("\n%s implementation:\n", impl);
            System.out.println("  Strengths: " + String.join(", ", strengths));
            System.out.println("  Best for: " + determineUseCase(strengths));
        });
    }

    private String determineUseCase(List<String> strengths) {
        if (strengths.stream().allMatch(s -> s.startsWith("COMPLEX"))) {
            return "Complex query workloads";
        } else if (strengths.stream().anyMatch(s -> s.equals("SELECT"))) {
            return "Read-heavy workloads";
        } else if (strengths.stream().anyMatch(s -> s.equals("INSERT"))) {
            return "Write-heavy workloads";
        } else {
            return "Mixed workloads";
        }
    }

    private void printTypeStatistics(List<TestResults> runResults) {
        // Calculate and print average latencies
        Map<String, List<Long>> allLatencies = new HashMap<>();
        runResults.forEach(run -> run.getOperationLatencies().forEach(
                (op, latencies) -> allLatencies.computeIfAbsent(op, k -> new ArrayList<>()).addAll(latencies)));

        allLatencies.forEach((op, latencies) -> {
            DoubleSummaryStatistics stats = latencies.stream()
                    .mapToDouble(l -> l / 1_000_000.0) // Convert to ms
                    .summaryStatistics();

            System.out.printf("\n%s Operations:\n", op);
            System.out.printf("  Average: %.3f ms\n", stats.getAverage());
            System.out.printf("  Min: %.3f ms\n", stats.getMin());
            System.out.printf("  Max: %.3f ms\n", stats.getMax());
            System.out.printf("  Count: %d\n", stats.getCount());
        });

        // Calculate and print memory statistics
        DoubleSummaryStatistics memStats = runResults.stream()
                .mapToDouble(r -> r.getPeakMemoryUsage() / (1024.0 * 1024.0)) // Convert to MB
                .summaryStatistics();

        System.out.printf("\nMemory Usage:\n");
        System.out.printf("  Average: %.2f MB\n", memStats.getAverage());
        System.out.printf("  Peak: %.2f MB\n", memStats.getMax());
    }

    private void evaluateConditionalQueries(String tableName, DataType dataType, TestResults results) {
        for (int i = 0; i < 100; i++) {
            String conditionType = getConditionType(dataType); // Capture the condition type
            String conditionQuery = generateConditionalQuery(tableName, dataType, conditionType);

            long start = System.nanoTime();
            dbEngine.executeSQL(conditionQuery);
            results.recordLatency(conditionType, System.nanoTime() - start); // Use conditionType as the key
        }
    }

    // Helper method to generate a condition type based on the DataType
    private String getConditionType(DataType dataType) {
        switch (dataType) {
            case SMALL_INTEGER, LARGE_INTEGER -> {
                return "GREATER_THAN_CONDITION";
            }
            case BOOLEAN -> {
                return "BOOLEAN_CONDITION";
            }
            case DATE -> {
                return "DATE_CONDITION";
            }
            default -> {
                return "UNKNOWN_CONDITION";
            }
        }
    }

    // Modify the generateConditionalQuery method to accept conditionType
    private String generateConditionalQuery(String tableName, DataType dataType, String conditionType) {
        String condition;
        switch (conditionType) {
            case "GREATER_THAN_CONDITION" -> condition = "value > '50' AND value < '100'"; // Treat integers as strings
            case "BOOLEAN_CONDITION" -> condition = "value = 'true'"; // Treat booleans as strings
            case "DATE_CONDITION" -> condition = "value > '2022-01-01'"; // Treat dates as strings
            default -> condition = "1=1"; // Fallback if no specific condition type
        }
        return String.format("SELECT * FROM %s WHERE %s", tableName, condition);
    }

    private void generateConditionalMetricsCSV(String baseDir) throws IOException {
        Path filePath = Paths.get(baseDir, "conditional_metrics.csv");

        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            // Write header
            writer.write("Condition,Implementation_DataType,Average_Time_ms,Sample_Count");
            writer.newLine();

            results.forEach((impl, typeResults) -> {
                typeResults.forEach((dataType, runResults) -> {
                    String implDataType = impl + "_" + dataType;

                    // Process each condition's latencies
                    runResults.forEach(run -> {
                        run.getOperationLatencies().forEach((conditionType, latencies) -> { // conditionType is now the
                                                                                            // key
                            if (latencies != null && !latencies.isEmpty()) {
                                double avgLatency = latencies.stream()
                                        .mapToDouble(l -> l / 1_000_000.0)
                                        .average()
                                        .orElse(0.0);

                                try {
                                    writer.write(String.format("%s,%s,%.3f,%d",
                                            conditionType, // Write the actual condition type
                                            implDataType,
                                            avgLatency,
                                            latencies.size()));
                                    writer.newLine();
                                } catch (IOException e) {
                                    throw new RuntimeException("Error writing conditional data", e);
                                }
                            }
                        });
                    });
                });
            });
        }
    }

    private void executeFrequencyTest(String tableName, DataType dataType, TestResults results) {
        for (int i = 0; i < TEST_OPERATIONS; i++) {
            int key = zipfianGenerator.nextInt(1000); // Use Zipfian distribution for hot-spot access
            String query = String.format("SELECT * FROM %s WHERE id = %d", tableName, key);

            // System.out.println("Executing frequency test query: " + query);

            long start = System.nanoTime();
            dbEngine.executeSQL(query);
            results.recordLatency("FREQUENCY_TEST", System.nanoTime() - start);
        }
    }

    private void executeSequentialTest(String tableName, DataType dataType, TestResults results) {
        for (int i = 0; i < 1000; i++) {
            String query = String.format("SELECT * FROM %s WHERE id = %d", tableName, i);
            System.out.println("Query: " + query);

            long start = System.nanoTime();
            String queryResults = dbEngine.executeSQL(query);
            results.recordLatency("SEQUENTIAL_TEST", System.nanoTime() - start);

            System.out.println("Sequential test query result: " +
                    (queryResults.isEmpty() ? "No results found" :
                            queryResults));
        }
    }

    private void executeRangeTest(String tableName, DataType dataType, TestResults results) {
        for (int i = 0; i < 100; i++) {
            int lowerBound = random.nextInt(900);
            int upperBound = lowerBound + 100;
            String query = String.format("SELECT * FROM %s WHERE id BETWEEN %d AND %d", tableName, lowerBound,
                    upperBound);

            long start = System.nanoTime();
            dbEngine.executeSQL(query);
            results.recordLatency("RANGE_TEST", System.nanoTime() - start);
        }
    }

    private void generateAccessPatternMetricsCSV(String baseDir) throws IOException {
        generatePatternMetricsCSV(baseDir, "frequency_metrics.csv", "FREQUENCY_TEST");
        generatePatternMetricsCSV(baseDir, "sequential_metrics.csv", "SEQUENTIAL_TEST");
        generatePatternMetricsCSV(baseDir, "range_metrics.csv", "RANGE_TEST");
    }

    private void generatePatternMetricsCSV(String baseDir, String fileName, String patternType) throws IOException {
        Path filePath = Paths.get(baseDir, fileName);

        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            // Write header
            writer.write("Condition,Implementation_DataType,Average_Time_ms,Sample_Count");
            writer.newLine();

            results.forEach((impl, typeResults) -> {
                typeResults.forEach((dataType, runResults) -> {
                    String implDataType = impl + "_" + dataType;

                    // Process each pattern's latencies
                    runResults.forEach(run -> {
                        List<Long> latencies = run.getOperationLatencies().get(patternType);
                        if (latencies != null && !latencies.isEmpty()) {
                            double avgLatency = latencies.stream()
                                    .mapToDouble(l -> l / 1_000_000.0)
                                    .average()
                                    .orElse(0.0);

                            try {
                                writer.write(String.format("%s,%s,%.3f,%d",
                                        patternType,
                                        implDataType,
                                        avgLatency,
                                        latencies.size()));
                                writer.newLine();
                                System.out.printf("Wrote %s data for %s: AvgLatency=%.3f ms, SampleCount=%d%n",
                                        patternType, implDataType, avgLatency, latencies.size());
                            } catch (IOException e) {
                                throw new RuntimeException("Error writing " + patternType + " data", e);
                            }
                        } else {
                            System.out.printf("No data recorded for %s in %s%n", patternType, implDataType);
                        }
                    });
                });
            });
        }

        System.out.println("Results for " + patternType + " written to " + filePath);
    }
}
