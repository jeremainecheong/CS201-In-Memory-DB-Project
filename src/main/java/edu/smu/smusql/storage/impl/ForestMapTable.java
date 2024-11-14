package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;

import java.util.*;

public class ForestMapTable implements Table {
    private Map<String, TreeMap<Comparable<?>, List<Node>>> columns = new HashMap<>();
    private final List<String> columnNames = new ArrayList<>();

    public ForestMapTable(List<String> columns) {
        for (String column : columns) {
            this.columns.put(column, new TreeMap<>());
            columnNames.add(column);
        }
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columnNames);
    }

    @Override
    public void insert(List<String> values) {
        Node[] rowNodes = new Node[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Comparable<?> parsedValue = parseValue(values.get(i));
            String columnName = columnNames.get(i);
            // System.out.println("Inserting value: " + parsedValue + " into column: " + columnName);

            Node newNode = new Node(parsedValue);
            rowNodes[i] = newNode;

            if (i > 0) {
                rowNodes[i - 1].right = newNode;
                newNode.left = rowNodes[i - 1];
            }

            columns.computeIfAbsent(columnName, k -> new TreeMap<>())
                   .computeIfAbsent(parsedValue, k -> new ArrayList<>())
                   .add(newNode);
        }
        
        // Print table state after each insertion
        // System.out.println("Table State After Insert:");
        // printTableState();
    }

    private void printTableState() {
        for (String column : columnNames) {
            System.out.println("Column: " + column);
            TreeMap<Comparable<?>, List<Node>> columnMap = columns.get(column);
            for (Map.Entry<Comparable<?>, List<Node>> entry : columnMap.entrySet()) {
                System.out.print("  Value: " + entry.getKey() + " -> Rows: ");
                for (Node node : entry.getValue()) {
                    System.out.print(traverseRow(node) + " | ");
                }
                System.out.println();
            }
        }
        System.out.println("-------------------------------");
    }

    private String traverseRow(Node startNode) {
        StringBuilder row = new StringBuilder();
        Node currentNode = startNode;
        while (currentNode.left != null) currentNode = currentNode.left;

        while (currentNode != null) {
            row.append(currentNode.value).append(" ");
            currentNode = currentNode.right;
        }
        return row.toString().trim();
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();
    
        // Retrieve all rows if no conditions are specified
        if (conditions.isEmpty()) {
            for (List<Node> nodeList : columns.get(columnNames.get(0)).values()) {
                for (Node node : nodeList) {
                    results.add(retrieveRow(node));
                }
            }
        } else {
            // Gather all row candidates based on each condition
            Set<Map<String, String>> candidateRows = new HashSet<>();
            boolean isOr = false;
            boolean hasConditions = false;
    
            for (String[] condition : conditions) {
                if (condition[0] != null) { // AND/OR
                    if (condition[0].equals("OR")) {
                        isOr = true;
                    }
                    continue;
                }
    
                String column = condition[1];
                String operator = condition[2];
                Comparable<?> value = parseValue(condition[3]);
                // System.out.println("Selecting column: " + column + " with operator: " + operator + " and value: " + value);
    
                TreeMap<Comparable<?>, List<Node>> columnMap = columns.get(column);
                if (columnMap == null) continue; // Skip if column not found
    
                SortedMap<Comparable<?>, List<Node>> filteredResults = switch (operator) {
                    case "=" -> columnMap.subMap(value, true, value, true);
                    case ">" -> columnMap.tailMap(value, false);
                    case "<" -> columnMap.headMap(value, false);
                    case ">=" -> columnMap.tailMap(value, true);
                    case "<=" -> columnMap.headMap(value, true);
                    default -> new TreeMap<>();
                };
    
                // Collect candidate rows based on current condition
                Set<Map<String, String>> conditionResults = new HashSet<>();
                for (List<Node> nodeList : filteredResults.values()) {
                    for (Node node : nodeList) {
                        conditionResults.add(retrieveRow(node));
                    }
                }
    
                if (hasConditions) {
                    // Apply AND/OR logic
                    if (isOr) {
                        candidateRows.addAll(conditionResults); // OR adds more rows
                        isOr = false; // Reset OR for next conditions
                    } else {
                        candidateRows.retainAll(conditionResults); // AND narrows down rows
                    }
                } else {
                    candidateRows.addAll(conditionResults);
                    hasConditions = true;
                }
            }
    
            // Evaluate and add matching rows based on final candidate rows and conditions
            for (Map<String, String> row : candidateRows) {
                if (evaluateConditions(row, conditions)) {
                    results.add(row);
                }
            }
        }
    
        return results;
    }
    
    private boolean evaluateConditions(Map<String, String> row, List<String[]> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return true;
        }
    
        boolean result = true;
        boolean isOr = false;
    
        for (String[] condition : conditions) {
            if (condition[0] != null) { // AND/OR
                if (condition[0].equals("OR")) {
                    isOr = true;
                }
                continue;
            }
    
            String column = condition[1];
            String operator = condition[2];
            String value = condition[3];
            boolean matches = evaluateCondition(row.get(column), operator, value);
    
            if (isOr) {
                result = result || matches;
                isOr = false;
            } else {
                result = result && matches;
            }
        }
    
        return result;
    }
    
    private boolean evaluateCondition(String rowValueStr, String operator, String valueStr) {
        Comparable<?> rowValue = parseValue(rowValueStr);
        Comparable<?> value = parseValue(valueStr);
    
        int comparison = ((Comparable<Object>) rowValue).compareTo(value);
    
        return switch (operator) {
            case "=" -> comparison == 0;
            case ">" -> comparison > 0;
            case "<" -> comparison < 0;
            case ">=" -> comparison >= 0;
            case "<=" -> comparison <= 0;
            default -> false;
        };
    }

    @Override
    public int update(String column, String value, List<String[]> conditions) {
        int updatedCount = 0;
        Comparable<?> newValue = parseValue(value);

        for (Map<String, String> row : select(conditions)) {
            // System.out.println("Attempting to update row: " + row);

            Node targetNode = findNode(row, column);
            if (targetNode != null) {
                Comparable<?> oldValue = targetNode.value;
                // System.out.println("Updating column '" + column + "' from " + oldValue + " to " + newValue);

                // Update the node's value
                targetNode.value = newValue;

                // Update the TreeMap entries to reflect the new value
                TreeMap<Comparable<?>, List<Node>> columnMap = columns.get(column);
                columnMap.get(oldValue).remove(targetNode);
                if (columnMap.get(oldValue).isEmpty()) {
                    columnMap.remove(oldValue);
                }

                columnMap.computeIfAbsent(newValue, k -> new ArrayList<>()).add(targetNode);
                updatedCount++;

                // Print the updated row for verification
                // System.out.println("Updated row: " + retrieveRow(findNode(row, columnNames.get(0))));
            } else {
                // System.out.println("No matching row found for update conditions.");
            }
        }

        // System.out.println("Total rows updated: " + updatedCount);
        // Print table state after each update
        // System.out.println("Table State After UPDATE:");
        // printTableState();
        return updatedCount;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int deletedCount = 0;
        List<Map<String, String>> rowsToDelete = select(conditions);

        for (Map<String, String> row : rowsToDelete) {
            // System.out.println("Attempting to delete row: " + row);

            Node startNode = findNode(row, columnNames.get(0));
            if (startNode != null) {
                // Print the row just before deletion
                // System.out.println("Deleting row: " + retrieveRow(startNode));

                deleteRow(startNode);
                deletedCount++;
            } else {
                // System.out.println("No matching row found for delete conditions.");
            }
        }

        // System.out.println("Total rows deleted: " + deletedCount);
        // Print table state after each deletion
        // System.out.println("Table State After DELETE:");
        // printTableState();
        return deletedCount;
    }


    private Map<String, String> retrieveRow(Node startNode) {
        Map<String, String> row = new HashMap<>();
        Node currentNode = startNode;
        while (currentNode != null && currentNode.left != null) currentNode = currentNode.left;

        for (String column : columnNames) {
            if (currentNode == null) break;
            row.put(column, String.valueOf(currentNode.value));
            currentNode = currentNode.right;
        }
        return row;
    }

    private Node findNode(Map<String, String> row, String targetColumn) {
        int targetIndex = columnNames.indexOf(targetColumn);
    
        // Start by getting nodes from the first column that match the first column's value in the row
        Comparable<?> startValue = parseValue(row.get(columnNames.get(0)));
        List<Node> nodes = columns.get(columnNames.get(0)).get(startValue);
    
        // If no nodes are found for the first column's value, return null (no match)
        if (nodes == null || nodes.isEmpty()) {
            return null;
        }
    
        // If only one node exists, skip to full row validation
        if (nodes.size() == 1) {
            Node singleCandidate = nodes.get(0);
            return validateAndGetTargetNode(singleCandidate, row, targetIndex);
        }
    
        // If multiple nodes exist, iterate through each candidate to find a matching row
        for (Node candidateNode : nodes) {
            Node targetNode = validateAndGetTargetNode(candidateNode, row, targetIndex);
            if (targetNode != null) {
                return targetNode;
            }
        }
    
        // Return null if no matching row is found
        return null;
    }
    
    private Node validateAndGetTargetNode(Node startNode, Map<String, String> row, int targetIndex) {
        Node currentNode = startNode;
    
        // Traverse the row from the first column, validating each column's value
        for (int i = 0; i < columnNames.size(); i++) {
            String column = columnNames.get(i);
            Comparable<?> expectedValue = parseValue(row.get(column));
    
            // If a mismatch occurs, this is not the correct row
            if (!currentNode.value.equals(expectedValue)) {
                return null;
            }
    
            // Move to the right node if this is not the target column
            if (i < columnNames.size() - 1) {
                currentNode = currentNode.right;
            }
        }
    
        // If all values matched, traverse to the target column node
        currentNode = startNode;
        for (int i = 0; i < targetIndex; i++) {
            currentNode = currentNode.right;
        }
        return currentNode;
    }

    private void deleteRow(Node startNode) {
        Node currentNode = startNode;
        while (currentNode.left != null) currentNode = currentNode.left;

        for (String column : columnNames) {
            columns.get(column).get(currentNode.value).remove(currentNode);
            if (columns.get(column).get(currentNode.value).isEmpty()) {
                columns.get(column).remove(currentNode.value);
            }
            currentNode = currentNode.right;
        }
    }

    private Comparable<?> parseValue(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e1) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e2) {
                return value;
            }
        }
    }    

    class Node {
        Comparable<?> value;
        Node left;
        Node right;

        Node(Comparable<?> value) {
            this.value = value;
        }
    }
}