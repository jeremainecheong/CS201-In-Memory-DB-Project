package edu.smu.smusql.test;

import edu.smu.smusql.storage.Table;
import java.util.*;
import java.util.function.Predicate;

/**
 * Provides testing utilities for Table implementations
 */
class TestFramework {
    static class TestCase {
        final String name;
        final Runnable test;

        TestCase(String name, Runnable test) {
            this.name = name;
            this.test = test;
        }
    }

    static void printData(String message, List<Map<String, String>> data) {
        System.out.println("\n" + message);
        System.out.println("-".repeat(50));
        if (data.isEmpty()) {
            System.out.println("No data");
            return;
        }

        // Print headers
        String format = "%-6s %-15s %-6s%n";
        System.out.printf(format, "ID", "NAME", "GPA");
        System.out.println("-".repeat(50));

        // Print rows
        for (Map<String, String> row : data) {
            System.out.printf(format,
                    row.getOrDefault("id", "null"),
                    row.getOrDefault("name", "null"),
                    row.getOrDefault("gpa", "null")
            );
        }
        System.out.println();
    }

    static void verifyData(List<Map<String, String>> actual,
                           List<Map<String, String>> expected,
                           String message) {
        if (actual.size() != expected.size()) {
            printData("Actual Data:", actual);
            printData("Expected Data:", expected);
            throw new AssertionError(String.format("%s: Size mismatch. Expected %d, got %d",
                    message, expected.size(), actual.size()));
        }

        for (int i = 0; i < expected.size(); i++) {
            Map<String, String> expectedRow = expected.get(i);
            Map<String, String> actualRow = actual.get(i);

            for (String column : expectedRow.keySet()) {
                if (!expectedRow.get(column).equals(actualRow.get(column))) {
                    printData("Actual Data:", actual);
                    printData("Expected Data:", expected);
                    throw new AssertionError(String.format("%s: Data mismatch at row %d, column %s. " +
                                    "Expected %s, got %s", message, i, column,
                            expectedRow.get(column), actualRow.get(column)));
                }
            }
        }
    }

    static void verifyConditions(List<Map<String, String>> results,
                                 Map<String, Predicate<String>> conditions,
                                 String message) {
        for (Map<String, String> row : results) {
            for (Map.Entry<String, Predicate<String>> condition : conditions.entrySet()) {
                String column = condition.getKey();
                Predicate<String> test = condition.getValue();

                if (!test.test(row.get(column))) {
                    printData("Failed Results:", results);
                    throw new AssertionError(String.format("%s: Condition failed for column %s, " +
                            "value %s", message, column, row.get(column)));
                }
            }
        }
    }
}