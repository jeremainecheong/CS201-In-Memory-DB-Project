package edu.smu.smusql;

import edu.smu.smusql.evaluation.Evaluator;
import java.util.Scanner;

public class Main {
    private static Engine dbEngine = new Engine();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("smuSQL Database System by G2T2");
        System.out.println("=====================");
        System.out.println("Available commands:");
        System.out.println("- SQL queries (CREATE TABLE, INSERT, SELECT, UPDATE, DELETE)");
        System.out.println("- evaluate (runs performance evaluation)");
        System.out.println("- exit");
        System.out.println();

        while (true) {
            System.out.print("smusql> ");
            String input = scanner.nextLine().trim();

            try {
                if (input.equalsIgnoreCase("exit")) {
                    break;
                } else if (input.equalsIgnoreCase("evaluate")) {
                    runEvaluation();
                } else if (!input.isEmpty()) {
                    String result = dbEngine.executeSQL(input);
                    System.out.println(result);
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }

        scanner.close();
        System.out.println("Goodbye!");
    }

    private static void runEvaluation() {
        System.out.println("\nStarting Performance Evaluation");
        System.out.println("=============================");

        try {
            Evaluator evaluator = new Evaluator(dbEngine);
            evaluator.runEvaluation();
        } catch (Exception e) {
            System.err.println("Evaluation failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}