package edu.smu.smusql.evaluation;

import edu.smu.smusql.Engine;
import edu.smu.smusql.evaluation.metrics.*;
import edu.smu.smusql.evaluation.generators.*;
import edu.smu.smusql.evaluation.operations.*;

import java.util.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public class Evaluator {
    private final Engine dbEngine;
    private final Random random;
    private final int NUMBER_OF_RUNS = 5;
    private final int WARMUP_OPERATIONS = 1000;
    private final int TEST_OPERATIONS = 10000;

    // Statistical tracking
    private final Map<String, List<MetricsSnapshot>> implementationMetrics;
    private final MemoryMXBean memoryBean;

    public Evaluator(Engine engine) {
        this.dbEngine = engine;
        this.random = new Random();
        this.implementationMetrics = new HashMap<>();
        this.memoryBean = ManagementFactory.getMemoryMXBean();
    }

    public void runEvaluation() {
        String[] implementations = {"BackwardsStack", "LeakyBucket", "PingPong", "RandomQueue"};

        for (String implementation : implementations) {
            System.out.println("\nEvaluating " + implementation);
            implementationMetrics.put(implementation, new ArrayList<>());

            // Run multiple times for statistical significance
            for (int run = 0; run < NUMBER_OF_RUNS; run++) {
                System.out.println("\nRun " + (run + 1) + " of " + NUMBER_OF_RUNS);

                // Reset database and warm up
                dbEngine.reset(); // Assuming you add a reset method to Engine
                warmup(implementation);

                // Force garbage collection between runs
                System.gc();

                // Actual test run
                MetricsSnapshot metrics = runTestPhases(implementation);
                implementationMetrics.get(implementation).add(metrics);
            }
        }

        // Print final statistics
        printStatistics();
    }

    private void warmup(String implementation) {
        System.out.println("Warming up...");
        WorkloadGenerator generator = new WorkloadGenerator(0.8); // 80% reads, 20% writes

        for (int i = 0; i < WARMUP_OPERATIONS; i++) {
            generator.nextOperation(implementation).execute(dbEngine);
        }
    }

    private MetricsSnapshot runTestPhases(String implementation) {
        MetricsSnapshot metrics = new MetricsSnapshot();

        // Phase 1: Initial Population
        metrics.populationPhase = runPopulationPhase(implementation);

        // Phase 2: Mixed Operations
        metrics.mixedOperationsPhase = runMixedOperationsPhase(implementation);

        // Phase 3: Complex Queries
        metrics.complexQueriesPhase = runComplexQueriesPhase(implementation);

        return metrics;
    }

    private PhaseMetrics runPopulationPhase(String implementation) {
        System.out.println("Running population phase...");
        PhaseMetrics metrics = new PhaseMetrics();
        String tablePrefix = implementation.toLowerCase() + "_";

        long startTime = System.nanoTime();
        MemoryUsage beforeMemory = memoryBean.getHeapMemoryUsage();

        // Create tables
        createTables(tablePrefix);

        // Generate and insert initial data with various distributions
        DataGenerator dataGen = new DataGenerator();
        for (int i = 0; i < 1000; i++) {
            // Users with age distribution
            Map<String, Object> userData = dataGen.generateUserData(i);
            String userQuery = generateInsertQuery(tablePrefix + "users", userData);
            long opStart = System.nanoTime();
            dbEngine.executeSQL(userQuery);
            metrics.recordOperationLatency("INSERT", System.nanoTime() - opStart);

            // Products with price distribution
            Map<String, Object> productData = dataGen.generateProductData(i);
            String productQuery = generateInsertQuery(tablePrefix + "products", productData);
            opStart = System.nanoTime();
            dbEngine.executeSQL(productQuery);
            metrics.recordOperationLatency("INSERT", System.nanoTime() - opStart);

            // Orders with quantity distribution
            Map<String, Object> orderData = dataGen.generateOrderData(i);
            String orderQuery = generateInsertQuery(tablePrefix + "orders", orderData);
            opStart = System.nanoTime();
            dbEngine.executeSQL(orderQuery);
            metrics.recordOperationLatency("INSERT", System.nanoTime() - opStart);
        }

        metrics.totalTime = System.nanoTime() - startTime;
        metrics.memoryUsed = memoryBean.getHeapMemoryUsage().getUsed() - beforeMemory.getUsed();

        return metrics;
    }

    private PhaseMetrics runMixedOperationsPhase(String implementation) {
        System.out.println("Running mixed operations phase...");
        PhaseMetrics metrics = new PhaseMetrics();
        String tablePrefix = implementation.toLowerCase() + "_";

        WorkloadGenerator workloadGen = new WorkloadGenerator(0.7); // 70% reads, 30% writes
        long startTime = System.nanoTime();
        MemoryUsage beforeMemory = memoryBean.getHeapMemoryUsage();

        for (int i = 0; i < TEST_OPERATIONS; i++) {
            DatabaseOperation operation = workloadGen.nextOperation(tablePrefix);
            long opStart = System.nanoTime();
            operation.execute(dbEngine);
            metrics.recordOperationLatency(operation.getType(), System.nanoTime() - opStart);
        }

        metrics.totalTime = System.nanoTime() - startTime;
        metrics.memoryUsed = memoryBean.getHeapMemoryUsage().getUsed() - beforeMemory.getUsed();

        return metrics;
    }

    private PhaseMetrics runComplexQueriesPhase(String implementation) {
        System.out.println("Running complex queries phase...");
        PhaseMetrics metrics = new PhaseMetrics();
        String tablePrefix = implementation.toLowerCase() + "_";

        ComplexQueryGenerator queryGen = new ComplexQueryGenerator(tablePrefix);
        long startTime = System.nanoTime();
        MemoryUsage beforeMemory = memoryBean.getHeapMemoryUsage();

        for (int i = 0; i < 1000; i++) {
            String query = queryGen.generateComplexQuery();
            long opStart = System.nanoTime();
            dbEngine.executeSQL(query);
            metrics.recordOperationLatency("COMPLEX_QUERY", System.nanoTime() - opStart);
        }

        metrics.totalTime = System.nanoTime() - startTime;
        metrics.memoryUsed = memoryBean.getHeapMemoryUsage().getUsed() - beforeMemory.getUsed();

        return metrics;
    }

    private void createTables(String tablePrefix) {
        dbEngine.executeSQL("CREATE TABLE " + tablePrefix + "users (id, name, age, city)");
        dbEngine.executeSQL("CREATE TABLE " + tablePrefix + "products (id, name, price, category)");
        dbEngine.executeSQL("CREATE TABLE " + tablePrefix + "orders (id, user_id, product_id, quantity)");
    }

    private String generateInsertQuery(String tableName, Map<String, Object> data) {
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(tableName).append(" VALUES (");

        boolean first = true;
        for (Object value : data.values()) {
            if (!first) {
                query.append(", ");
            }
            if (value instanceof String) {
                query.append("'").append(value).append("'");
            } else {
                query.append(value);
            }
            first = false;
        }
        query.append(")");

        return query.toString();
    }

    private void printStatistics() {
        System.out.println("\nFinal Statistics");
        System.out.println("================");

        for (String implementation : implementationMetrics.keySet()) {
            System.out.println("\n" + implementation + " Statistics:");
            List<MetricsSnapshot> runs = implementationMetrics.get(implementation);

            // Calculate averages and standard deviations
            printPhaseStatistics("Population Phase", runs, r -> r.populationPhase);
            printPhaseStatistics("Mixed Operations Phase", runs, r -> r.mixedOperationsPhase);
            printPhaseStatistics("Complex Queries Phase", runs, r -> r.complexQueriesPhase);
        }
    }

    private void printPhaseStatistics(String phaseName, List<MetricsSnapshot> runs,
                                      java.util.function.Function<MetricsSnapshot, PhaseMetrics> phaseExtractor) {
        System.out.println("\n" + phaseName + ":");

        // Calculate statistics
        double avgTime = runs.stream()
                .mapToDouble(r -> phaseExtractor.apply(r).totalTime / 1_000_000_000.0)
                .average()
                .orElse(0.0);

        double stdDev = calculateStdDev(runs.stream()
                .mapToDouble(r -> phaseExtractor.apply(r).totalTime / 1_000_000_000.0)
                .toArray());

        double avgMemory = runs.stream()
                .mapToDouble(r -> phaseExtractor.apply(r).memoryUsed / (1024.0 * 1024.0))
                .average()
                .orElse(0.0);

        System.out.printf("Average Time: %.3f seconds (±%.3f)\n", avgTime, stdDev);
        System.out.printf("Average Memory Usage: %.2f MB\n", avgMemory);

        // Print latency percentiles
        printLatencyPercentiles(runs, phaseExtractor);
    }

    private double calculateStdDev(double[] values) {
        double mean = Arrays.stream(values).average().orElse(0.0);
        double variance = Arrays.stream(values)
                .map(v -> Math.pow(v - mean, 2))
                .average()
                .orElse(0.0);
        return Math.sqrt(variance);
    }

    private void printLatencyPercentiles(List<MetricsSnapshot> runs,
                                         java.util.function.Function<MetricsSnapshot, PhaseMetrics> phaseExtractor) {
        // Combine latencies from all runs
        Map<String, List<Long>> allLatencies = new HashMap<>();

        for (MetricsSnapshot run : runs) {
            PhaseMetrics phase = phaseExtractor.apply(run);
            phase.operationLatencies.forEach((op, latencies) -> {
                allLatencies.computeIfAbsent(op, k -> new ArrayList<>()).addAll(latencies);
            });
        }

        // Calculate and print percentiles for each operation type
        allLatencies.forEach((op, latencies) -> {
            Collections.sort(latencies);
            int size = latencies.size();
            System.out.println("\nLatencies for " + op + ":");
            System.out.printf("50th percentile: %.3f ms\n", latencies.get(size/2) / 1_000_000.0);
            System.out.printf("90th percentile: %.3f ms\n", latencies.get((int)(size * 0.9)) / 1_000_000.0);
            System.out.printf("99th percentile: %.3f ms\n", latencies.get((int)(size * 0.99)) / 1_000_000.0);
        });
    }
}