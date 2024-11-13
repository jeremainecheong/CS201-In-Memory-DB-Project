package edu.smu.smusql;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.impl.BackwardsStackTable;
import edu.smu.smusql.storage.impl.LeakyBucketTable;
import edu.smu.smusql.storage.impl.PingPongTable;
import edu.smu.smusql.storage.impl.RandomQueueTable;
import edu.smu.smusql.storage.impl.ForestMapTable;
import edu.smu.smusql.storage.monitor.TableMonitor;

import java.util.*;

public class Engine {
    private Map<String, Table> tables = new HashMap<>();
    private Parser parser = new Parser();

    /**
     * Resets the engine state, clearing all tables
     */
    public void reset() {
        tables.clear();
    }

    // untouched
    public String executeSQL(String query) {
        String[] tokens = query.trim().split("\\s+");
        String command = tokens[0].toUpperCase();

        switch (command) {
            case "CREATE":
                return create(tokens);
            case "INSERT":
                return insert(tokens);
            case "SELECT":
                return select(tokens);
            case "UPDATE":
                return update(tokens);
            case "DELETE":
                return delete(tokens);
            case "STATS":
                return getStats(tokens);
            default:
                return "ERROR: Unknown command";
        }
    }

    private String getStats(String[] tokens) {
        if (tokens.length < 2) {
            return "ERROR: Table name required for STATS";
        }
        String tableName = tokens[1];
        Table table = tables.get(tableName);
        if (table == null) {
            return "ERROR: Table '" + tableName + "' does not exist";
        }
        if (table instanceof TableMonitor) {
            return ((TableMonitor) table).getPerformanceReport();
        }
        return "ERROR: Table is not monitored";
    }
    /**
     * Creates a table with specified implementation based on prefix
     */
    public String create(String[] tokens) {
        if (tokens.length < 3 || !tokens[1].equalsIgnoreCase("TABLE")) {
            return "ERROR: Invalid CREATE TABLE syntax";
        }

        String tableName = tokens[2];
        if (tables.containsKey(tableName)) {
            return "ERROR: Table '" + tableName + "' already exists";
        }

        // Parse column list
        StringBuilder columnsStr = new StringBuilder();
        boolean inParens = false;
        for (int i = 3; i < tokens.length; i++) {
            if (tokens[i].startsWith("(")) {
                inParens = true;
                columnsStr.append(tokens[i].substring(1)).append(" ");
            } else if (tokens[i].endsWith(")")) {
                columnsStr.append(tokens[i].substring(0, tokens[i].length() - 1));
                break;
            } else if (inParens) {
                columnsStr.append(tokens[i]).append(" ");
            }
        }

        List<String> columns = Arrays.asList(columnsStr.toString().trim().split(","));
        for (int i = 0; i < columns.size(); i++) {
            columns.set(i, columns.get(i).trim());
        }

        // Create table with specific implementation
        Table baseTable;
        String monitorName;
        String prefix = tableName.toLowerCase();

        // Use explicit prefix checks for evaluation framework
        if (prefix.startsWith("backwards_")) {
            baseTable = new BackwardsStackTable(columns);
            monitorName = "BackwardsStack";
        } else if (prefix.startsWith("ping_")) {
            baseTable = new PingPongTable(columns);
            monitorName = "PingPong";
        } else if (prefix.startsWith("random_")) {
            baseTable = new RandomQueueTable(columns);
            monitorName = "RandomQueue";
        } else if (prefix.startsWith("leaky_")) {
            baseTable = new LeakyBucketTable(columns);
            monitorName = "LeakyBucket";
        } else if (prefix.startsWith("forest_")) {
            baseTable = new ForestMapTable(columns);
            monitorName = "ForestMap";
        } else {
            // Default to LeakyBucket for unspecified implementations
            baseTable = new LeakyBucketTable(columns);
            monitorName = "LeakyBucket";
        }

        // Wrap with monitor
        Table monitoredTable = new TableMonitor(baseTable, monitorName);
        tables.put(tableName, monitoredTable);

        return "Table '" + tableName + "' created with " + monitorName + " implementation";
    }

    /**
     * Inserts a row into a table with performance monitoring
     */
    public String insert(String[] tokens) {
        try {
            String tableName = tokens[2];
            Table table = tables.get(tableName);
            if (table == null) {
                return "ERROR: Table '" + tableName + "' does not exist";
            }

            // Extract values more efficiently
            String valuesStr = extractParenthesesContent(tokens, 4);
            if (valuesStr == null) {
                return "ERROR: Invalid INSERT syntax";
            }

            List<String> values = Arrays.asList(valuesStr.split(","));
            for (int i = 0; i < values.size(); i++) {
                values.set(i, values.get(i).trim());
            }

            table.insert(values);
            return "Row inserted successfully";
        } catch (Exception e) {
            return "ERROR: Insert failed - " + e.getMessage();
        }
    }

    /**
     * Executes a SELECT query with improved error handling
     */
    public String select(String[] tokens) {
        try {
            if (tokens.length < 4 || !tokens[1].equals("*") || !tokens[2].equalsIgnoreCase("FROM")) {
                return "ERROR: Invalid SELECT syntax";
            }

            String tableName = tokens[3];
            Table table = tables.get(tableName);
            if (table == null) {
                return "ERROR: Table '" + tableName + "' does not exist";
            }

            List<String[]> conditions = parseConditions(tokens, 4);
            List<Map<String, String>> results = table.select(conditions);

            return formatResults(results, table.getColumns());
        } catch (Exception e) {
            return "ERROR: Select failed - " + e.getMessage();
        }
    }

    /**
     * Executes an UPDATE query with better condition parsing
     */
    public String update(String[] tokens) {
        try {
            String tableName = tokens[1];
            Table table = tables.get(tableName);
            if (table == null) {
                return "ERROR: Table '" + tableName + "' does not exist";
            }

            String column = tokens[3];
            String value = tokens[5];
            List<String[]> conditions = parseConditions(tokens, 6);

            int updatedRows = table.update(column, value, conditions);
            return updatedRows + " rows updated";
        } catch (Exception e) {
            return "ERROR: Update failed - " + e.getMessage();
        }
    }

    /**
     * Executes a DELETE query with improved condition handling
     */
    public String delete(String[] tokens) {
        try {
            String tableName = tokens[2];
            Table table = tables.get(tableName);
            if (table == null) {
                return "ERROR: Table '" + tableName + "' does not exist";
            }

            List<String[]> conditions = parseConditions(tokens, 3);
            int deletedRows = table.delete(conditions);
            return deletedRows + " rows deleted";
        } catch (Exception e) {
            return "ERROR: Delete failed - " + e.getMessage();
        }
    }

    // Helper methods
    private String extractParenthesesContent(String[] tokens, int startIndex) {
        StringBuilder content = new StringBuilder();
        boolean inParens = false;

        for (int i = startIndex; i < tokens.length; i++) {
            if (tokens[i].startsWith("(")) {
                inParens = true;
                content.append(tokens[i].substring(1)).append(" ");
            } else if (tokens[i].endsWith(")")) {
                content.append(tokens[i].substring(0, tokens[i].length() - 1));
                break;
            } else if (inParens) {
                content.append(tokens[i]).append(" ");
            }
        }

        return inParens ? content.toString().trim() : null;
    }

    private List<String[]> parseConditions(String[] tokens, int startIndex) {
        List<String[]> conditions = new ArrayList<>();

        if (tokens.length > startIndex && tokens[startIndex].equalsIgnoreCase("WHERE")) {
            for (int i = startIndex + 1; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                    conditions.add(new String[] {tokens[i].toUpperCase(), null, null, null});
                } else if (isOperator(tokens[i])) {
                    conditions.add(new String[] {null, tokens[i-1], tokens[i], tokens[i+1]});
                    i++;
                }
            }
        }

        return conditions;
    }

    private String formatResults(List<Map<String, String>> results, List<String> columns) {
        if (results.isEmpty()) {
            return "No results found";
        }

        StringBuilder output = new StringBuilder();
        output.append(String.join("\t", columns)).append("\n");

        for (Map<String, String> row : results) {
            for (String column : columns) {
                output.append(row.get(column)).append("\t");
            }
            output.append("\n");
        }

        return output.toString();
    }

    private boolean isOperator(String token) {
        return token.equals("=") || token.equals(">") ||
                token.equals("<") || token.equals(">=") ||
                token.equals("<=");
    }
}
