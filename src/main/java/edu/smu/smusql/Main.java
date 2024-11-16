package edu.smu.smusql;

import edu.smu.smusql.evaluation.*;
import java.util.Scanner;
import java.time.Duration;
import java.time.Instant;

public class Main {
    private static Engine dbEngine = new Engine();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Welcome to smuSQL Database System by G2T2!");
        System.out.println("===========================================");
        System.out.println("This is your gateway to a lightweight, yet powerful SQL interface.");
        System.out.println("With smuSQL, you can create tables, insert data, run queries, and much more.");
        System.out.println();
        System.out.println("Key Features:");
        System.out.println("- Execute SQL commands to interact with your database.");
        System.out.println("- Type 'evaluate' to run a comprehensive performance evaluation.");
        System.out.println("- Use 'help' to see the full list of supported commands.");
        System.out.println("- Type 'exit' to safely quit the program.");
        System.out.println();
        System.out.println("Ready to explore the world of smuSQL? Let's get started!");
        System.out.println();

        while (true) {
            System.out.print("smusql> ");
            String input = scanner.nextLine().trim();

            try {
                if (input.equalsIgnoreCase("exit")) {
                    break;
                } else if (input.equalsIgnoreCase("evaluate")) {
                    runEvaluation(scanner);
                } else if (input.equalsIgnoreCase("help")) {
                    printCommands();
                } else if (!input.isEmpty()) {
                    String result = dbEngine.executeSQL(input);
                    System.out.println(result);
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                if (Boolean.getBoolean("smusql.debug")) {
                    e.printStackTrace();
                }
            }
        }

        scanner.close();
        System.out.println("Goodbye!");
    }

    private static void printCommands() {
        System.out.println("\nAvailable commands:");
        System.out.println("- SQL queries:");
        System.out.println("  CREATE TABLE table_name (column1, column2, ...)");
        System.out.println("  INSERT INTO table_name VALUES (value1, value2, ...)");
        System.out.println("  SELECT * FROM table_name [WHERE condition]");
        System.out.println("  UPDATE table_name SET column = value WHERE condition");
        System.out.println("  DELETE FROM table_name WHERE condition");
        System.out.println("- evaluate (runs comprehensive performance evaluation)");
        System.out.println("- help (shows this message)");
        System.out.println("- exit");
        System.out.println();
    }

    private static void runEvaluation(Scanner scanner) {
        System.out.println("\nStarting Comprehensive Performance Evaluation");
        System.out.println("=========================================");

        // Print evaluation details
        System.out.println("\nEvaluation will test:");
        System.out.println("- 3 implementations (ForestMap, Chunk, LFUTable)");
        System.out.println("- 8 data types (varying string lengths, integers, decimals, etc.)");
        System.out.println("- Multiple operations (INSERT, SELECT, UPDATE, DELETE)");
        System.out.println("- Memory usage and operation latencies");
        System.out.println("- An optional scalability test (100, 1000, 10000, 30000 rows)");

        // Ask for confirmation
        System.out.print("\nThis evaluation will take several minutes. Continue? (y/n): ");
        String response = scanner.nextLine().trim().toLowerCase();

        if (!response.equals("y")) {
            System.out.println("Evaluation cancelled.");
            return;
        }
        try {
            Instant startTime = Instant.now();

            // Create and run evaluator
            Evaluator evaluator = new Evaluator(dbEngine);
            evaluator.runEvaluation(scanner);

            // Print completion statistics
            Instant endTime = Instant.now();
            Duration duration = Duration.between(startTime, endTime);

            System.out.println("\nEvaluation completed successfully!");
            System.out.printf("Total time: %d minutes, %d seconds\n",
                    duration.toMinutes(),
                    duration.getSeconds() % 60);

        } catch (Exception e) {
            System.err.println("\nEvaluation failed!");
            System.err.println("Error: " + e.getMessage());

            if (Boolean.getBoolean("smusql.debug")) {
                e.printStackTrace();
            } else {
                System.err.println("Run with -Dsmusql.debug=true for stack trace");
            }
        }
    }
}