package edu.smu.smusql;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.impl.*;

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
            default:
                return "ERROR: Unknown command";
        }
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

        // Extract columns inside parentheses
        StringBuilder columnsString = new StringBuilder();
        boolean foundOpenParen = false;
        for (int i = 3; i < tokens.length; i++) {
            String token = tokens[i].trim(); // Trim each token
            if (token.startsWith("(")) {
                foundOpenParen = true;
                columnsString.append(token.substring(1)).append(" "); // Remove '('
            } else if (token.endsWith(")")) {
                columnsString.append(token, 0, token.length() - 1); // Remove ')'
                break;
            } else if (foundOpenParen) {
                columnsString.append(token).append(" ");
            }
        }

        if (!foundOpenParen || columnsString.length() == 0) {
            return "ERROR: No columns specified";
        }

        // Split columns and clean up names
        List<String> columns = new ArrayList<>();
        for (String col : columnsString.toString().split(",")) {
            String cleanCol = col.trim();
            if (!cleanCol.isEmpty()) {
                columns.add(cleanCol);
            }
        }

        if (columns.isEmpty()) {
            return "ERROR: No valid columns specified";
        }

        // Create the table with the correct implementation
        Table table;
        String implName;
        String prefix = tableName.toLowerCase();

        if (prefix.startsWith("backwards_")) {
            table = new BackwardsStackTable(columns);
            implName = "BackwardsStack";
        } else if (prefix.startsWith("ping_")) {
            table = new PingPongTable(columns);
            implName = "PingPong";
        } else if (prefix.startsWith("leaky_")) {
            table = new LeakyBucketTable(columns);
            implName = "LeakyBucket";
        } else if (prefix.startsWith("forest_")) {
            table = new ForestMapTable(columns);
            implName = "ForestMap";
        } else {
            table = new ChunkTable(columns);
            implName = "ChunkTable";
        }

        tables.put(tableName, table);
        return "Table '" + tableName + "' created with " + implName + " implementation";
    }


    /**
     * Inserts a row into a table
     */
    public String insert(String[] tokens) {
        try {
            if (tokens.length < 4 || !tokens[1].equalsIgnoreCase("INTO")) {
                return "ERROR: Invalid INSERT syntax";
            }

            String tableName = tokens[2];
            Table table = tables.get(tableName);
            if (table == null) {
                return "ERROR: Table '" + tableName + "' does not exist";
            }

            // Extract values inside parentheses
            StringBuilder valuesStr = new StringBuilder();
            boolean inParens = false;
            for (int i = 4; i < tokens.length; i++) {
                String token = tokens[i].trim(); // Trim each token
                if (token.startsWith("(")) {
                    inParens = true;
                    valuesStr.append(token.substring(1)).append(" "); // Remove '('
                } else if (token.endsWith(")")) {
                    valuesStr.append(token, 0, token.length() - 1); // Remove ')'
                    break;
                } else if (inParens) {
                    valuesStr.append(token).append(" ");
                }
            }

            if (!inParens || valuesStr.length() == 0) {
                return "ERROR: Invalid INSERT syntax - missing values";
            }

            // Split values and clean up
            List<String> values = new ArrayList<>();
            for (String value : valuesStr.toString().split(",")) {
                String cleanValue = value.trim();
                if (!cleanValue.isEmpty()) {
                    values.add(cleanValue);
                }
            }

            if (values.size() != table.getColumns().size()) {
                return "ERROR: Number of values does not match number of columns";
            }

            table.insert(values);
            return "Row inserted successfully";
        } catch (Exception e) {
            return "ERROR: Insert failed - " + e.getMessage();
        }
    }


    /**
     * Executes a SELECT query
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
     * Executes an UPDATE query
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
     * Executes a DELETE query
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

    private List<String[]> parseConditions(String[] tokens, int startIndex) {
        List<String[]> conditions = new ArrayList<>();

        if (tokens.length > startIndex && tokens[startIndex].equalsIgnoreCase("WHERE")) {
            String logicalOp = "AND";  // Default logical operator for the first condition
            for (int i = startIndex + 1; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                    logicalOp = tokens[i].toUpperCase();
                } else if (isOperator(tokens[i])) {
                    String column = tokens[i - 1];
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    conditions.add(new String[] {logicalOp, column, operator, value});
                    i++; // Skip over the value token
                    logicalOp = "AND"; // Reset logical operator after use
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
