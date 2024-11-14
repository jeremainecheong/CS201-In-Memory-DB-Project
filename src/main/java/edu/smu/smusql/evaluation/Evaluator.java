package edu.smu.smusql.evaluation;

import edu.smu.smusql.Engine;
import edu.smu.smusql.evaluation.metrics.DataType;
import edu.smu.smusql.evaluation.metrics.TestResults;
import edu.smu.smusql.evaluation.generators.ValueGenerator;
import edu.smu.smusql.evaluation.operations.StandardQueryExecutor;
import edu.smu.smusql.evaluation.operations.ComplexQueryExecutor;

import java.io.IOException;
import java.util.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.io.BufferedWriter;
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

    public Evaluator(Engine engine) {
        this.dbEngine = engine;
        this.random = new Random();
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.results = new ConcurrentHashMap<>();
        this.valueGenerator = new ValueGenerator();
        this.standardExecutor = new StandardQueryExecutor();
        this.complexExecutor = new ComplexQueryExecutor();
    }

    public void runEvaluation() {
        String[] implementations = {"backwards_", "chunk_", "forest_", "random_"};

        for (String impl : implementations) {
            System.out.println("\nEvaluating implementation: " + impl);
            results.put(impl, new EnumMap<>(DataType.class));

            for (DataType type : DataType.values()) {
                evaluateImplementationWithDataType(impl, type);
            }
        }

        generateReport();
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

        // Mixed operations phase
        executeMixedOperations(tableName, dataType, results);

        // Complex queries phase
        executeComplexQueries(tableName, dataType, results);

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
            generateCSVReports();
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
        results.values().forEach(typeResults ->
                typeResults.values().forEach(runResults ->
                        runResults.forEach(run ->
                                operations.addAll(run.getOperationLatencies().keySet()))));

        for (String operation : operations) {
            Map<String, Double> implPerformance = new HashMap<>();

            results.forEach((impl, typeResults) -> {
                typeResults.forEach((dataType, runResults) -> {
                    String implDataType = impl + "_" + dataType;
                    double avgLatency = runResults.stream()
                            .flatMap(run -> run.getOperationLatencies().getOrDefault(operation, new ArrayList<>()).stream())
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
                "COMPLEX_SELECT", "COMPLEX_UPDATE", "COMPLEX_DELETE"
        ));

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
                        .collect(Collectors.toSet())
        );

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
                    .limit(3)  // Top 3 performers
                    .forEach(entry -> {
                        String impl = entry.getKey().split("_")[0];  // Get implementation name
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
        runResults.forEach(run ->
            run.getOperationLatencies().forEach((op, latencies) ->
                allLatencies.computeIfAbsent(op, k -> new ArrayList<>()).addAll(latencies)));

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
}
