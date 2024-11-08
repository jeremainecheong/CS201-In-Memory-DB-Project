package edu.smu.smusql;

import edu.smu.smusql.storage.Table;
import edu.smu.smusql.storage.impl.BackwardsStackTable;
import edu.smu.smusql.storage.impl.LeakyBucketTable;
import edu.smu.smusql.storage.impl.PingPongTable;
import edu.smu.smusql.storage.impl.RandomQueueTable;
import edu.smu.smusql.storage.monitor.TableMonitor;

import java.util.*;

public class Engine {
    private Map<String, Table> tables = new HashMap<>();
    private Parser parser = new Parser();

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

    public String create(String[] tokens) {
        if (tokens.length < 3 || !tokens[1].equalsIgnoreCase("TABLE")) {
            return "ERROR: Invalid CREATE TABLE syntax";
        }

        String tableName = tokens[2];
        if (tables.containsKey(tableName)) {
            return "ERROR: Table '" + tableName + "' already exists";
        }

        // Parse the column list
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

        // Create table with monitoring based on table name prefix
        Table baseTable;
        String monitorName;
        
        if (tableName.toLowerCase().startsWith("backwards") || tableName.toLowerCase().startsWith("stack")) {
            baseTable = new BackwardsStackTable(columns);
            monitorName = "BackwardsStack";
        } else if (tableName.toLowerCase().startsWith("ping") || tableName.toLowerCase().startsWith("pong")) {
            baseTable = new PingPongTable(columns);
            monitorName = "PingPong";
        } else if (tableName.toLowerCase().startsWith("random") || tableName.toLowerCase().startsWith("queue")) {
            baseTable = new RandomQueueTable(columns);
            monitorName = "RandomQueue";
        } else {
            baseTable = new LeakyBucketTable(columns);
            monitorName = "LeakyBucket";
        }

        // Wrap with monitor
        Table monitoredTable = new TableMonitor(baseTable, monitorName);
        tables.put(tableName, monitoredTable);

        return "Table '" + tableName + "' created with " + monitorName + " implementation";
    }

    public String insert(String[] tokens) {
        parser.parseInsert(tokens);  // Validate syntax

        String tableName = tokens[2];
        Table table = tables.get(tableName);
        if (table == null) {
            return "ERROR: Table '" + tableName + "' does not exist";
        }

        // Extract values
        StringBuilder valuesStr = new StringBuilder();
        boolean inParens = false;
        for (int i = 4; i < tokens.length; i++) {
            if (tokens[i].startsWith("(")) {
                inParens = true;
                valuesStr.append(tokens[i].substring(1)).append(" ");
            } else if (tokens[i].endsWith(")")) {
                valuesStr.append(tokens[i].substring(0, tokens[i].length() - 1));
                break;
            } else if (inParens) {
                valuesStr.append(tokens[i]).append(" ");
            }
        }

        List<String> values = Arrays.asList(valuesStr.toString().trim().split(","));
        for (int i = 0; i < values.size(); i++) {
            values.set(i, values.get(i).trim());
        }

        table.insert(values);
        return "Row inserted successfully";
    }

    public String select(String[] tokens) {
        if (tokens.length < 4 || !tokens[1].equals("*") ||
                !tokens[2].equalsIgnoreCase("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }

        String tableName = tokens[3];
        Table table = tables.get(tableName);
        if (table == null) {
            return "ERROR: Table '" + tableName + "' does not exist";
        }

        // Parse WHERE conditions
        List<String[]> conditions = new ArrayList<>();
        if (tokens.length > 4 && tokens[4].equalsIgnoreCase("WHERE")) {
            for (int i = 5; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                    conditions.add(new String[] {tokens[i].toUpperCase(), null, null, null});
                } else if (isOperator(tokens[i])) {
                    conditions.add(new String[] {null, tokens[i-1], tokens[i], tokens[i+1]});
                    i++;
                }
            }
        }

        List<Map<String, String>> results = table.select(conditions);

        if (results.isEmpty()) {
            return "No results found";
        }

        StringBuilder output = new StringBuilder();
        output.append(String.join("\t", table.getColumns())).append("\n");

        for (Map<String, String> row : results) {
            for (String column : table.getColumns()) {
                output.append(row.get(column)).append("\t");
            }
            output.append("\n");
        }

        return output.toString();
    }

    public String update(String[] tokens) {
        parser.parseUpdate(tokens);  // Validate syntax

        String tableName = tokens[1];
        Table table = tables.get(tableName);
        if (table == null) {
            return "ERROR: Table '" + tableName + "' does not exist";
        }

        String column = tokens[3];
        String value = tokens[5];

        // Parse WHERE conditions
        List<String[]> conditions = new ArrayList<>();
        if (tokens.length > 6 && tokens[6].equalsIgnoreCase("WHERE")) {
            for (int i = 7; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                    conditions.add(new String[] {tokens[i].toUpperCase(), null, null, null});
                } else if (isOperator(tokens[i])) {
                    conditions.add(new String[] {null, tokens[i-1], tokens[i], tokens[i+1]});
                    i++;
                }
            }
        }

        int updatedRows = table.update(column, value, conditions);
        return updatedRows + " rows updated";
    }

    public String delete(String[] tokens) {
        parser.parseDelete(tokens);  // Validate syntax

        String tableName = tokens[2];
        Table table = tables.get(tableName);
        if (table == null) {
            return "ERROR: Table '" + tableName + "' does not exist";
        }

        // Parse WHERE conditions
        List<String[]> conditions = new ArrayList<>();
        if (tokens.length > 3 && tokens[3].equalsIgnoreCase("WHERE")) {
            for (int i = 4; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                    conditions.add(new String[] {tokens[i].toUpperCase(), null, null, null});
                } else if (isOperator(tokens[i])) {
                    conditions.add(new String[] {null, tokens[i-1], tokens[i], tokens[i+1]});
                    i++;
                }
            }
        }

        int deletedRows = table.delete(conditions);
        return deletedRows + " rows deleted";
    }

    private boolean isOperator(String token) {
        return token.equals("=") || token.equals(">") ||
                token.equals("<") || token.equals(">=") ||
                token.equals("<=");
    }
}
