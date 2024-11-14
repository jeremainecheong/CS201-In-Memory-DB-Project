package edu.smu.smusql;

import edu.smu.smusql.evaluation.*;
import java.util.Scanner;
import java.time.Duration;
import java.time.Instant;

public class Main {
    private static Engine dbEngine = new Engine();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("smuSQL Database System by G2T2");
        System.out.println("=====================");

        while (true) {
            System.out.print("smusql> ");
            String input = scanner.nextLine().trim();

            try {
                if (input.equalsIgnoreCase("exit")) {
                    break;
                } else if (input.equalsIgnoreCase("evaluate")) {
                    runEvaluation();
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

    private static void runEvaluation() {
        System.out.println("\nStarting Comprehensive Performance Evaluation");
        System.out.println("=========================================");
        
        // Print evaluation details
        System.out.println("\nEvaluation will test:");
        System.out.println("- 4 implementations (BackwardsStack, Chunk, ForestMap, RandomQueue)");
        System.out.println("- 8 data types (varying string lengths, integers, decimals, etc.)");
        System.out.println("- Multiple operations (INSERT, SELECT, UPDATE, DELETE)");
        System.out.println("- Memory usage and operation latencies");
        
        // Ask for confirmation
        System.out.print("\nThis evaluation will take several minutes. Continue? (y/n): ");
        Scanner scanner = new Scanner(System.in);
        String response = scanner.nextLine().trim().toLowerCase();
        
        if (!response.equals("y")) {
            System.out.println("Evaluation cancelled.");
            return;
        }

        try {
            Instant startTime = Instant.now();

            // Create and run evaluator
            Evaluator evaluator = new Evaluator(dbEngine);
            evaluator.runEvaluation();

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