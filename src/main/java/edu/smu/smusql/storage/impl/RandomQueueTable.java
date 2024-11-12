package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import java.util.*;

public class RandomQueueTable implements Table {
    private final List<String> columns;
    private final List<Queue<Map<String, String>>> queues;  // Multiple queues
    private final Random random = new Random();
    private static final int NUM_QUEUES = 4;  // Number of queues to distribute data
    private static final int REBALANCE_THRESHOLD = 5000;  // Less frequent rebalancing
    private static final double SIZE_IMBALANCE_THRESHOLD = 2.0;  // Only rebalance if size difference is significant
    private int operationCount = 0;
    private int totalRows = 0;

    public RandomQueueTable(List<String> columns) {
        this.columns = new ArrayList<>(columns);
        this.queues = new ArrayList<>(NUM_QUEUES);
        for (int i = 0; i < NUM_QUEUES; i++) {
            queues.add(new LinkedList<>());
        }
    }

    @Override
    public void insert(List<String> values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }

        // Use consistent hashing-like approach for distribution
        int hash = Math.abs(row.hashCode());
        int queueIndex = hash % NUM_QUEUES;
        queues.get(queueIndex).offer(row);
        totalRows++;

        operationCount++;
        if (operationCount >= REBALANCE_THRESHOLD && shouldRebalance()) {
            rebalanceQueues();
            operationCount = 0;
        }
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();
        
        // Process queues in parallel using temporary storage
        List<Queue<Map<String, String>>> tempQueues = new ArrayList<>(NUM_QUEUES);
        for (int i = 0; i < NUM_QUEUES; i++) {
            tempQueues.add(new LinkedList<>());
        }

        // Process each queue
        for (int i = 0; i < NUM_QUEUES; i++) {
            Queue<Map<String, String>> queue = queues.get(i);
            Queue<Map<String, String>> tempQueue = tempQueues.get(i);

            while (!queue.isEmpty()) {
                Map<String, String> row = queue.poll();
                if (evaluateConditions(row, conditions)) {
                    results.add(new HashMap<>(row));
                }
                tempQueue.offer(row);
            }
        }

        // Restore all queues
        for (int i = 0; i < NUM_QUEUES; i++) {
            queues.set(i, tempQueues.get(i));
        }

        operationCount++;
        return results;
    }

    @Override
    public int update(String column, String value, List<String[]> conditions) {
        int count = 0;
        List<Queue<Map<String, String>>> tempQueues = new ArrayList<>(NUM_QUEUES);
        for (int i = 0; i < NUM_QUEUES; i++) {
            tempQueues.add(new LinkedList<>());
        }

        // Process each queue
        for (int i = 0; i < NUM_QUEUES; i++) {
            Queue<Map<String, String>> queue = queues.get(i);
            Queue<Map<String, String>> tempQueue = tempQueues.get(i);

            while (!queue.isEmpty()) {
                Map<String, String> row = queue.poll();
                if (evaluateConditions(row, conditions)) {
                    row.put(column, value);
                    count++;
                }
                tempQueue.offer(row);
            }
        }

        // Restore all queues
        for (int i = 0; i < NUM_QUEUES; i++) {
            queues.set(i, tempQueues.get(i));
        }

        operationCount++;
        return count;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int count = 0;
        List<Queue<Map<String, String>>> tempQueues = new ArrayList<>(NUM_QUEUES);
        for (int i = 0; i < NUM_QUEUES; i++) {
            tempQueues.add(new LinkedList<>());
        }

        // Process each queue
        for (int i = 0; i < NUM_QUEUES; i++) {
            Queue<Map<String, String>> queue = queues.get(i);
            Queue<Map<String, String>> tempQueue = tempQueues.get(i);

            while (!queue.isEmpty()) {
                Map<String, String> row = queue.poll();
                if (!evaluateConditions(row, conditions)) {
                    tempQueue.offer(row);
                } else {
                    count++;
                    totalRows--;
                }
            }
        }

        // Restore all queues
        for (int i = 0; i < NUM_QUEUES; i++) {
            queues.set(i, tempQueues.get(i));
        }

        // Only rebalance after significant deletes
        if (count > totalRows / 4) {
            rebalanceQueues();
        }

        operationCount++;
        return count;
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    private boolean shouldRebalance() {
        int minSize = Integer.MAX_VALUE;
        int maxSize = 0;
        
        for (Queue<Map<String, String>> queue : queues) {
            int size = queue.size();
            minSize = Math.min(minSize, size);
            maxSize = Math.max(maxSize, size);
        }

        // Only rebalance if there's significant imbalance
        return maxSize > minSize * SIZE_IMBALANCE_THRESHOLD;
    }

    private void rebalanceQueues() {
        // Collect all rows while maintaining their relative order in each queue
        List<List<Map<String, String>>> queueData = new ArrayList<>();
        for (Queue<Map<String, String>> queue : queues) {
            List<Map<String, String>> queueRows = new ArrayList<>();
            while (!queue.isEmpty()) {
                queueRows.add(queue.poll());
            }
            queueData.add(queueRows);
        }

        // Calculate target size per queue
        int totalSize = totalRows;
        int targetSize = totalSize / NUM_QUEUES;
        int remainder = totalSize % NUM_QUEUES;

        // Redistribute rows while trying to maintain original queue affinity
        int currentQueue = 0;
        for (List<Map<String, String>> queueRows : queueData) {
            for (Map<String, String> row : queueRows) {
                // Try to keep row in original queue if possible
                int preferredQueue = Math.abs(row.hashCode()) % NUM_QUEUES;
                if (queues.get(preferredQueue).size() < targetSize + (preferredQueue < remainder ? 1 : 0)) {
                    queues.get(preferredQueue).offer(row);
                } else {
                    // Find next available queue
                    while (queues.get(currentQueue).size() >= targetSize + (currentQueue < remainder ? 1 : 0)) {
                        currentQueue = (currentQueue + 1) % NUM_QUEUES;
                    }
                    queues.get(currentQueue).offer(row);
                }
            }
        }
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

    private boolean evaluateCondition(String rowValue, String operator, String value) {
        try {
            double rowNum = Double.parseDouble(rowValue);
            double valueNum = Double.parseDouble(value);

            return switch (operator) {
                case "=" -> rowNum == valueNum;
                case ">" -> rowNum > valueNum;
                case "<" -> rowNum < valueNum;
                case ">=" -> rowNum >= valueNum;
                case "<=" -> rowNum <= valueNum;
                default -> false;
            };
        } catch (NumberFormatException e) {
            return switch (operator) {
                case "=" -> rowValue.equals(value);
                default -> false;
            };
        }
    }
}
