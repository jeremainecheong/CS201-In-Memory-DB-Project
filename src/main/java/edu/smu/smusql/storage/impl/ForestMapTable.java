package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.DataType;
import edu.smu.smusql.storage.Table;

import java.util.*;

/**
 * ForestMapTable implements a multi-index structure using TreeMaps for each column.
 * Optimized for range queries and complex conditions through synchronized tree-based indexes
 * with bidirectional node links between column values.
 */
public class ForestMapTable implements Table {
    private Map<String, TreeMap<DataType, List<Node>>> columns = new HashMap<>();
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

    /**
     * Inserts a new row into the table by creating linked nodes across all column indexes.
     * Maintains bidirectional relationships between column values for efficient traversal.
     *
     * @param values List of string values corresponding to each column
     */
    @Override
    public void insert(List<String> values) {
        if (values.size() != columnNames.size()) {
            throw new IllegalArgumentException("Value count does not match column count.");
        }

        Node[] rowNodes = new Node[values.size()];
        for (int i = 0; i < values.size(); i++) {
            DataType parsedValue = DataType.fromString(values.get(i));
            String columnName = columnNames.get(i);

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
    }

    /**
     * Performs optimized selection using tree-based indexes.
     * Efficiently handles range queries and complex conditions using TreeMap operations.
     *
     * @param conditions List of condition arrays: [logical_op, column, operator, value]
     * @return List of maps representing matching rows
     */
    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();

        if (conditions.isEmpty()) {
            // Retrieve all rows
            TreeMap<DataType, List<Node>> firstColumnMap = columns.get(columnNames.get(0));
            if (firstColumnMap != null) {
                for (List<Node> nodeList : firstColumnMap.values()) {
                    for (Node node : nodeList) {
                        results.add(retrieveRow(node));
                    }
                }
            }
            return results;
        }

        // Gather all row candidates based on each condition
        Set<Map<String, String>> candidateRows = new HashSet<>();
        boolean isFirstCondition = true;

        for (String[] condition : conditions) {
            String logicalOp = condition[0];
            String column = condition[1];
            String operator = condition[2];
            String valueStr = condition[3];

            if (column == null || operator == null || valueStr == null) {
                continue; // Skip invalid conditions
            }

            DataType value = DataType.fromString(valueStr);

            TreeMap<DataType, List<Node>> columnMap = columns.get(column);
            if (columnMap == null) continue; // Skip if column not found

            SortedMap<DataType, List<Node>> filteredResults = switch (operator) {
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

            if (isFirstCondition) {
                candidateRows.addAll(conditionResults);
                isFirstCondition = false;
            } else {
                if (logicalOp.equalsIgnoreCase("OR")) {
                    candidateRows.addAll(conditionResults);
                } else if (logicalOp.equalsIgnoreCase("AND")) {
                    candidateRows.retainAll(conditionResults);
                }
            }
        }

        // Evaluate and add matching rows based on final candidate rows and conditions
        for (Map<String, String> row : candidateRows) {
            if (evaluateConditions(row, conditions)) {
                results.add(row);
            }
        }

        return results;
    }

    /**
     * Evaluates a set of conditions against a row.
     * Handles complex logical combinations of AND/OR conditions.
     *
     * @param row Map of column names to values
     * @param conditions List of condition arrays
     * @return True if row matches conditions, false otherwise
     */
    private boolean evaluateConditions(Map<String, String> row, List<String[]> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return true;
        }

        boolean result = false;
        boolean isFirstCondition = true;
        String currentLogicalOp = "AND"; // Default logical operator

        for (String[] condition : conditions) {
            String logicalOp = condition[0];
            String column = condition[1];
            String operator = condition[2];
            String valueStr = condition[3];

            if (column == null || operator == null || valueStr == null) {
                continue; // Skip invalid conditions
            }

            boolean matches = evaluateCondition(row.get(column), operator, valueStr);

            if (isFirstCondition) {
                result = matches;
                isFirstCondition = false;
            } else {
                if (logicalOp.equalsIgnoreCase("OR")) {
                    result = result || matches;
                } else if (logicalOp.equalsIgnoreCase("AND")) {
                    result = result && matches;
                }
            }
        }

        return result;
    }

    /**
     * Evaluates a single condition against a value.
     * Supports comparison operators: =, >, <, >=, <=
     *
     * @param rowValueStr Value from the row
     * @param operator Comparison operator
     * @param valueStr Value to compare against
     * @return True if condition is satisfied, false otherwise
     */
    private boolean evaluateCondition(String rowValueStr, String operator, String valueStr) {
        DataType rowValue = DataType.fromString(rowValueStr);
        DataType value = DataType.fromString(valueStr);

        int comparison = rowValue.compareTo(value);

        return switch (operator) {
            case "=" -> comparison == 0;
            case ">" -> comparison > 0;
            case "<" -> comparison < 0;
            case ">=" -> comparison >= 0;
            case "<=" -> comparison <= 0;
            default -> false;
        };
    }

    /**
     * Updates values while maintaining index consistency across all columns.
     * Handles index reorganization when values change.
     *
     * @param column Column to update
     * @param value New value for the column
     * @param conditions List of condition arrays: [logical_op, column, operator, value]
     * @return Number of rows updated
     */
    @Override
    public int update(String column, String value, List<String[]> conditions) {
        int updatedCount = 0;
        DataType newValue = DataType.fromString(value);

        for (Map<String, String> row : select(conditions)) {
            Node targetNode = findNode(row, column);
            if (targetNode != null) {
                DataType oldValue = targetNode.value;

                // Update the node's value
                targetNode.value = newValue;

                // Update the TreeMap entries to reflect the new value
                TreeMap<DataType, List<Node>> columnMap = columns.get(column);
                if (columnMap != null) {
                    List<Node> nodesWithOldValue = columnMap.get(oldValue);
                    if (nodesWithOldValue != null) {
                        nodesWithOldValue.remove(targetNode);
                        if (nodesWithOldValue.isEmpty()) {
                            columnMap.remove(oldValue);
                        }
                    }

                    columnMap.computeIfAbsent(newValue, k -> new ArrayList<>()).add(targetNode);
                    updatedCount++;
                }
            }
        }
        return updatedCount;
    }

    /**
     * Deletes rows and maintains index consistency across all columns.
     * Removes nodes from all relevant indexes.
     *
     * @param conditions List of condition arrays: [logical_op, column, operator, value]
     * @return Number of rows deleted
     */
    @Override
    public int delete(List<String[]> conditions) {
        int deletedCount = 0;
        List<Map<String, String>> rowsToDelete = select(conditions);

        for (Map<String, String> row : rowsToDelete) {
            Node startNode = findNode(row, columnNames.get(0));
            if (startNode != null) {
                deleteRow(startNode);
                deletedCount++;
            }
        }
        return deletedCount;
    }

    /**
     * Retrieves a complete row starting from a node.
     * Traverses bidirectional links to gather all column values.
     *
     * @param startNode Starting node of the row
     * @return Map of column names to values
     */
    private Map<String, String> retrieveRow(Node startNode) {
        Map<String, String> row = new HashMap<>();
        Node currentNode = startNode;
        while (currentNode != null && currentNode.left != null) currentNode = currentNode.left;

        for (String column : columnNames) {
            if (currentNode == null) break;
            row.put(column, currentNode.value.toString());
            currentNode = currentNode.right;
        }
        return row;
    }

    /**
     * Finds a specific node in the multi-index structure.
     * Uses primary key index for initial lookup and validates through links.
     *
     * @param row Row values to match
     * @param targetColumn Target column to find
     * @return Node if found, null otherwise
     */
    private Node findNode(Map<String, String> row, String targetColumn) {
        int targetIndex = columnNames.indexOf(targetColumn);

        if (targetIndex == -1) return null; // Column not found

        // Start by getting nodes from the first column that match the first column's value in the row
        DataType startValue = DataType.fromString(row.get(columnNames.get(0)));
        List<Node> nodes = columns.get(columnNames.get(0)).get(startValue);

        if (nodes == null || nodes.isEmpty()) {
            return null;
        }

        for (Node candidateNode : nodes) {
            Node targetNode = validateAndGetTargetNode(candidateNode, row, targetIndex);
            if (targetNode != null) {
                return targetNode;
            }
        }

        return null;
    }

    /**
     * Validates a node matches a row and returns the target column node.
     * Traverses through node links to ensure row integrity.
     *
     * @param startNode Initial node to start validation from
     * @param row Expected row values
     * @param targetIndex Index of target column
     * @return Target node if valid, null otherwise
     */
    private Node validateAndGetTargetNode(Node startNode, Map<String, String> row, int targetIndex) {
        Node currentNode = startNode;

        // Traverse the row from the first column, validating each column's value
        for (int i = 0; i < columnNames.size(); i++) {
            String column = columnNames.get(i);
            DataType expectedValue = DataType.fromString(row.get(column));

            if (!currentNode.value.equals(expectedValue)) {
                return null;
            }

            // Move to the right node if this is not the last column
            if (i < columnNames.size() - 1) {
                currentNode = currentNode.right;
                if (currentNode == null) {
                    return null; // Incomplete row
                }
            }
        }

        // Traverse to the target column node
        currentNode = startNode;
        for (int i = 0; i < targetIndex; i++) {
            currentNode = currentNode.right;
            if (currentNode == null) {
                return null; // Incomplete row
            }
        }
        return currentNode;
    }

    /**
     * Deletes a row from all indexes starting from a node.
     * Maintains index consistency by removing nodes from all column indexes.
     *
     * @param startNode Starting node of the row to delete
     */
    private void deleteRow(Node startNode) {
        Node currentNode = startNode;
        while (currentNode.left != null) currentNode = currentNode.left;

        for (String column : columnNames) {
            DataType value = currentNode.value;
            TreeMap<DataType, List<Node>> columnMap = columns.get(column);
            if (columnMap != null) {
                List<Node> nodes = columnMap.get(value);
                if (nodes != null) {
                    nodes.remove(currentNode);
                    if (nodes.isEmpty()) {
                        columnMap.remove(value);
                    }
                }
            }
            currentNode = currentNode.right;
            if (currentNode == null) break;
        }
    }

    /**
     * Inner class representing a node in the multi-index structure.
     * Maintains bidirectional links between column values.
     */
    class Node {
        DataType value;
        Node left;
        Node right;

        Node(DataType value) {
            this.value = value;
        }
    }
}
