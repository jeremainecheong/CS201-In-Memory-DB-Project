package edu.smu.smusql;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.impl.BackwardsStackTable;
import edu.smu.smusql.storage.impl.LeakyBucketTable;
import edu.smu.smusql.storage.impl.PingPongTable;
import edu.smu.smusql.storage.impl.RandomQueueTable;
import edu.smu.smusql.storage.impl.ForestMapTable;

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

        // Find the opening and closing parentheses
        String columnsString = "";
        boolean foundOpenParen = false;
        for (int i = 3; i < tokens.length; i++) {
            String token = tokens[i];
            if (token.startsWith("(")) {
                foundOpenParen = true;
                columnsString = token.substring(1);  // Remove opening parenthesis
            } else if (foundOpenParen) {
                if (token.endsWith(")")) {
                    // Remove closing parenthesis and add the last token
                    columnsString += " " + token.substring(0, token.length() - 1);
                    break;
                } else {
                    columnsString += " " + token;
                }
            }
        }

        if (columnsString.isEmpty()) {
            return "ERROR: No columns specified";
        }

        // Split the columns string and clean up each column name
        List<String> columns = new ArrayList<>();
        for (String col : columnsString.split(",")) {
            String cleanCol = col.trim();
            if (!cleanCol.isEmpty()) {
                columns.add(cleanCol);
            }
        }

        if (columns.isEmpty()) {
            return "ERROR: No valid columns specified";
        }

        // Create table with specific implementation
        Table table;
        String implName;
        String prefix = tableName.toLowerCase();

        // Use explicit prefix checks for evaluation framework
        if (prefix.startsWith("backwards_")) {
            table = new BackwardsStackTable(columns);
            implName = "BackwardsStack";
        } else if (prefix.startsWith("ping_")) {
            table = new PingPongTable(columns);
            implName = "PingPong";
        } else if (prefix.startsWith("random_")) {
            table = new RandomQueueTable(columns);
            implName = "RandomQueue";
        } else if (prefix.startsWith("leaky_")) {
            baseTable = new LeakyBucketTable(columns);
            implName = "LeakyBucket";
        } else if (prefix.startsWith("forest_")) {
            table = new ForestMapTable(columns);
            implName = "ForestMap";
        } else {
            // Default to LeakyBucket for unspecified implementations
            table = new LeakyBucketTable(columns);
            implName = "LeakyBucket";
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

            // Extract values
            StringBuilder valuesStr = new StringBuilder();
            boolean inParens = false;
            for (int i = 4; i < tokens.length; i++) {
                String token = tokens[i];
                if (token.startsWith("(")) {
                    inParens = true;
                    valuesStr.append(token.substring(1)).append(" ");
                } else if (token.endsWith(")")) {
                    valuesStr.append(token.substring(0, token.length() - 1));
                    break;
                } else if (inParens) {
                    valuesStr.append(token).append(" ");
                }
            }

            if (!inParens) {
                return "ERROR: Invalid INSERT syntax - missing values";
            }

            List<String> values = new ArrayList<>();
            for (String value : valuesStr.toString().split(",")) {
                values.add(value.trim());
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
