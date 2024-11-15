package edu.smu.smusql.test;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.impl.*;
import java.util.*;
import java.util.function.Predicate;

public class TableTest {
    private static final List<String> COLUMNS = Arrays.asList("id", "name", "gpa");

    // Test data as structured objects for better type safety and clarity
    private static class TestRow {
        final int id;
        final String name;
        final double gpa;

        TestRow(int id, String name, double gpa) {
            this.id = id;
            this.name = name;
            this.gpa = gpa;
        }

        List<String> toStringList() {
            return Arrays.asList(
                    String.valueOf(id),
                    name,
                    String.format("%.1f", gpa)
            );
        }

        Map<String, String> toMap() {
            Map<String, String> map = new LinkedHashMap<>();
            map.put("id", String.valueOf(id));
            map.put("name", name);
            map.put("gpa", String.format("%.1f", gpa));
            return map;
        }
    }

    private static final List<TestRow> INITIAL_DATA = Arrays.asList(
            new TestRow(1, "Alice", 3.5),
            new TestRow(2, "Bob", 3.8),
            new TestRow(3, "Charlie", 3.2),
            new TestRow(4, "David", 3.9),
            new TestRow(5, "Eve", 3.5),
            new TestRow(6, "Frank", 2.8),
            new TestRow(7, "Grace", 4.0),
            new TestRow(8, "Henry", 2.5),
            new TestRow(9, "Ivy", 3.7),
            new TestRow(10, "Jack", 3.3)
    );

    public static void main(String[] args) {
        List<Table> implementations = Arrays.asList(
                new ChunkTable(COLUMNS),
                new ForestMapTable(COLUMNS),
                new BackwardsStackTable(COLUMNS),
                new RandomQueueTable(COLUMNS),
                new LFUTable(COLUMNS)
        );

        for (Table table : implementations) {
            runTests(table);
        }
    }

    private static void runTests(Table table) {
        String implName = table.getClass().getSimpleName();
        System.out.println("\n=== Testing " + implName + " ===\n");

        List<TestFramework.TestCase> tests = Arrays.asList(
                new TestFramework.TestCase("Insert & Basic Select", () -> testInsert(table)),
                new TestFramework.TestCase("Single Equality", () -> testSimpleEquality(table)),
                new TestFramework.TestCase("Numeric Comparisons", () -> testNumericComparisons(table)),
                new TestFramework.TestCase("Multiple AND Conditions", () -> testMultipleAnd(table)),
                new TestFramework.TestCase("Multiple OR Conditions", () -> testMultipleOr(table)),
                new TestFramework.TestCase("Mixed AND/OR Conditions", () -> testMixedConditions(table)),
                new TestFramework.TestCase("Update Operations", () -> testUpdate(table)),
                new TestFramework.TestCase("Delete Operations", () -> testDelete(table)),
                new TestFramework.TestCase("Range Queries", () -> testRangeQueries(table))
        );

        int passed = 0;
        for (TestFramework.TestCase test : tests) {
            try {
                System.out.printf("%-25s: ", test.name);
                test.test.run();
                System.out.println("PASSED");
                passed++;
            } catch (AssertionError e) {
                System.out.println("FAILED");
                System.out.println("  " + e.getMessage());
            }
        }

        System.out.printf("\nResults for %s: %d/%d tests passed%n",
                implName, passed, tests.size());
    }

    private static void testInsert(Table table) {
        // Insert test data
        for (TestRow row : INITIAL_DATA) {
            table.insert(row.toStringList());
        }

        // Verify all data
        List<Map<String, String>> results = table.select(Collections.emptyList());
        List<Map<String, String>> expected = INITIAL_DATA.stream()
                .map(TestRow::toMap)
                .toList();

        TestFramework.verifyData(results, expected, "Basic insert");
    }

    private static void testSimpleEquality(Table table) {
        List<String[]> condition = new ArrayList<>();
        condition.add(new String[]{"AND", "name", "=", "Alice"});

        List<Map<String, String>> results = table.select(condition);

        Map<String, Predicate<String>> verify = new HashMap<>();
        verify.put("name", v -> v.equals("Alice"));
        verify.put("gpa", v -> v.equals("3.5"));

        TestFramework.verifyConditions(results, verify, "Simple equality");
    }

    private static void testNumericComparisons(Table table) {
        // Test greater than
        List<String[]> gtCondition = new ArrayList<>();
        gtCondition.add(new String[]{"AND", "gpa", ">", "3.5"});
        List<Map<String, String>> gtResults = table.select(gtCondition);

        Map<String, Predicate<String>> gtVerify = new HashMap<>();
        gtVerify.put("gpa", v -> Double.parseDouble(v) > 3.5);
        TestFramework.verifyConditions(gtResults, gtVerify, "Greater than");

        // Test less than
        List<String[]> ltCondition = new ArrayList<>();
        ltCondition.add(new String[]{"AND", "gpa", "<", "3.0"});
        List<Map<String, String>> ltResults = table.select(ltCondition);

        Map<String, Predicate<String>> ltVerify = new HashMap<>();
        ltVerify.put("gpa", v -> Double.parseDouble(v) < 3.0);
        TestFramework.verifyConditions(ltResults, ltVerify, "Less than");

        // Verify exact counts
        List<Map<String, String>> expectedGt = getExpectedRows(row -> row.gpa > 3.5);
        List<Map<String, String>> expectedLt = getExpectedRows(row -> row.gpa < 3.0);
        TestFramework.verifyData(gtResults, expectedGt, "Greater than count");
        TestFramework.verifyData(ltResults, expectedLt, "Less than count");
    }

    private static void testMultipleAnd(Table table) {
        List<String[]> conditions = new ArrayList<>();
        conditions.add(new String[]{"AND", "gpa", ">=", "3.5"});
        conditions.add(new String[]{"AND", "gpa", "<", "4.0"});
        conditions.add(new String[]{"AND", "id", "<=", "5"});

        List<Map<String, String>> results = table.select(conditions);

        // Verify all conditions are met
        Map<String, Predicate<String>> verify = new HashMap<>();
        verify.put("gpa", v -> {
            double gpa = Double.parseDouble(v);
            return gpa >= 3.5 && gpa < 4.0;
        });
        verify.put("id", v -> Integer.parseInt(v) <= 5);

        TestFramework.verifyConditions(results, verify, "Multiple AND");

        // Verify exact matches
        List<Map<String, String>> expected = getExpectedRows(row ->
                row.gpa >= 3.5 && row.gpa < 4.0 && row.id <= 5);
        TestFramework.verifyData(results, expected, "Multiple AND matches");
    }

    private static void testMultipleOr(Table table) {
        List<String[]> conditions = new ArrayList<>();
        conditions.add(new String[]{"OR", "gpa", "=", "4.0"});
        conditions.add(new String[]{"OR", "gpa", "<", "3.0"});
        conditions.add(new String[]{"OR", "name", "=", "Alice"});

        List<Map<String, String>> results = table.select(conditions);

        // Verify expected results are present
        List<Map<String, String>> expected = getExpectedRows(row ->
                row.gpa == 4.0 || row.gpa < 3.0 || row.name.equals("Alice"));
        TestFramework.verifyData(results, expected, "Multiple OR matches");

        // Additional verification for specific cases
        Set<String> expectedNames = new HashSet<>(Arrays.asList("Alice", "Grace", "Frank", "Henry"));
        for (Map<String, String> row : results) {
            String name = row.get("name");
            assert expectedNames.contains(name) :
                    "Unexpected name in results: " + name;
        }
    }

    private static void testMixedConditions(Table table) {
        List<String[]> conditions = new ArrayList<>();
        conditions.add(new String[]{"AND", "gpa", ">=", "3.5"});
        conditions.add(new String[]{"AND", "id", "<", "6"});
        conditions.add(new String[]{"OR", "name", "=", "Grace"});

        List<Map<String, String>> results = table.select(conditions);

        // Verify the complex condition logic
        List<Map<String, String>> expected = getExpectedRows(row ->
                (row.gpa >= 3.5 && row.id < 6) || row.name.equals("Grace"));
        TestFramework.verifyData(results, expected, "Mixed conditions");

        // Additional verification
        for (Map<String, String> row : results) {
            String name = row.get("name");
            double gpa = Double.parseDouble(row.get("gpa"));
            int id = Integer.parseInt(row.get("id"));

            assert name.equals("Grace") || (gpa >= 3.5 && id < 6) :
                    String.format("Invalid row: name=%s, gpa=%s, id=%s", name, gpa, id);
        }
    }

    private static void testUpdate(Table table) {
        // Test single row update
        List<String[]> singleCondition = new ArrayList<>();
        singleCondition.add(new String[]{"AND", "name", "=", "Charlie"});

        // Store original data for verification
        List<Map<String, String>> beforeUpdate = table.select(Collections.emptyList());

        int updated = table.update("gpa", "3.6", singleCondition);
        assert updated == 1 : "Expected 1 update, got " + updated;

        // Verify update
        List<Map<String, String>> results = table.select(singleCondition);
        assert results.size() == 1 : "Expected 1 result after update";
        assert results.get(0).get("gpa").equals("3.6") :
                "Expected updated GPA 3.6, got " + results.get(0).get("gpa");

        // Verify only target row was updated
        List<Map<String, String>> allResults = table.select(Collections.emptyList());
        assert allResults.size() == beforeUpdate.size() : "Row count changed after update";

        for (Map<String, String> row : allResults) {
            if (!row.get("name").equals("Charlie")) {
                int index = Integer.parseInt(row.get("id")) - 1;
                assert row.get("gpa").equals(beforeUpdate.get(index).get("gpa")) :
                        "Non-target row was modified: " + row.get("name");
            }
        }
    }

    private static void testDelete(Table table) {
        // Count initial rows
        List<Map<String, String>> beforeDelete = table.select(Collections.emptyList());
        int initialCount = beforeDelete.size();

        // Delete rows with GPA < 3.0
        List<String[]> conditions = new ArrayList<>();
        conditions.add(new String[]{"AND", "gpa", "<", "3.0"});

        // Store expected deleted rows for verification
        List<Map<String, String>> expectedDeleted = table.select(conditions);
        int expectedDeleteCount = expectedDeleted.size();

        int deleted = table.delete(conditions);
        assert deleted == expectedDeleteCount :
                String.format("Expected %d deletions, got %d", expectedDeleteCount, deleted);

        // Verify deletion
        List<Map<String, String>> afterDelete = table.select(conditions);
        assert afterDelete.isEmpty() :
                "Found " + afterDelete.size() + " rows matching delete condition after deletion";

        // Verify remaining rows
        List<Map<String, String>> remaining = table.select(Collections.emptyList());
        assert remaining.size() == initialCount - deleted :
                String.format("Expected %d rows after delete, found %d",
                        initialCount - deleted, remaining.size());

        // Verify the right rows were deleted
        for (Map<String, String> row : remaining) {
            double gpa = Double.parseDouble(row.get("gpa"));
            assert gpa >= 3.0 : "Found row with GPA < 3.0 after deletion: " + gpa;
        }
    }

    private static void testRangeQueries(Table table) {
        // Test GPA range with ID conditions
        List<String[]> conditions = new ArrayList<>();
        conditions.add(new String[]{"AND", "gpa", ">=", "3.2"});
        conditions.add(new String[]{"AND", "gpa", "<=", "3.8"});
        conditions.add(new String[]{"AND", "id", "<", "3"});
        conditions.add(new String[]{"OR", "id", ">", "8"});

        List<Map<String, String>> results = table.select(conditions);

        // Verify range conditions
        for (Map<String, String> row : results) {
            double gpa = Double.parseDouble(row.get("gpa"));
            int id = Integer.parseInt(row.get("id"));

            // Verify GPA range
            assert gpa >= 3.2 && gpa <= 3.8 :
                    "GPA out of range [3.2, 3.8]: " + gpa;

            // Verify ID conditions
            assert id < 3 || id > 8 :
                    "ID not meeting range conditions: " + id;
        }

        // Verify against expected results
        List<Map<String, String>> expected = getExpectedRows(row ->
                row.gpa >= 3.2 && row.gpa <= 3.8 && (row.id < 3 || row.id > 8));
        TestFramework.verifyData(results, expected, "Range query results");

        // Additional boundary testing
        conditions.clear();
        conditions.add(new String[]{"AND", "gpa", "=", "3.5"});
        results = table.select(conditions);

        // Should find exactly Alice and Eve
        assert results.size() == 2 : "Expected 2 results for GPA = 3.5, got " + results.size();
        Set<String> names = new HashSet<>();
        for (Map<String, String> row : results) {
            names.add(row.get("name"));
        }
        assert names.contains("Alice") && names.contains("Eve") :
                "Missing expected rows for exact GPA match";
    }

    private static List<Map<String, String>> getExpectedRows(Predicate<TestRow> filter) {
        return INITIAL_DATA.stream()
                .filter(filter)
                .map(TestRow::toMap)
                .toList();
    }
}