package edu.smu.smusql.storage.impl;

import edu.smu.smusql.storage.Table;
import java.util.*;

public class RandomQueueTable implements Table {
    private final List<String> columns;
    private final List<Queue<Map<String, String>>> queues;  // Multiple queues
    private final Random random = new Random();
    private static final int NUM_QUEUES = 4;  // Number of queues to distribute data
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

        // Randomly choose which queue to insert into
        int queueIndex = random.nextInt(NUM_QUEUES);
        queues.get(queueIndex).offer(row);
        totalRows++;

        // Occasional rebalancing
        if (shouldRebalance()) {
            rebalanceQueues();
        }
    }

    @Override
    public List<Map<String, String>> select(List<String[]> conditions) {
        List<Map<String, String>> results = new ArrayList<>();

        // Search queues in random order
        List<Integer> queueOrder = getRandomOrder();

        for (int queueIndex : queueOrder) {
            Queue<Map<String, String>> queue = queues.get(queueIndex);
            Queue<Map<String, String>> tempQueue = new LinkedList<>();

            // Process each queue
            while (!queue.isEmpty()) {
                Map<String, String> row = queue.poll();
                if (evaluateConditions(row, conditions)) {
                    results.add(new HashMap<>(row));
                }
                tempQueue.offer(row);
            }

            // Restore queue
            while (!tempQueue.isEmpty()) {
                queue.offer(tempQueue.poll());
            }
        }

        return results;
    }

    @Override
    public int update(String column, String value, List<String[]> conditions) {
        int count = 0;
        List<Integer> queueOrder = getRandomOrder();

        for (int queueIndex : queueOrder) {
            Queue<Map<String, String>> queue = queues.get(queueIndex);
            Queue<Map<String, String>> tempQueue = new LinkedList<>();

            while (!queue.isEmpty()) {
                Map<String, String> row = queue.poll();
                if (evaluateConditions(row, conditions)) {
                    row.put(column, value);
                    count++;
                }
                tempQueue.offer(row);
            }

            while (!tempQueue.isEmpty()) {
                queue.offer(tempQueue.poll());
            }
        }

        return count;
    }

    @Override
    public int delete(List<String[]> conditions) {
        int count = 0;
        List<Integer> queueOrder = getRandomOrder();

        for (int queueIndex : queueOrder) {
            Queue<Map<String, String>> queue = queues.get(queueIndex);
            Queue<Map<String, String>> tempQueue = new LinkedList<>();

            while (!queue.isEmpty()) {
                Map<String, String> row = queue.poll();
                if (!evaluateConditions(row, conditions)) {
                    tempQueue.offer(row);
                } else {
                    count++;
                    totalRows--;
                }
            }

            while (!tempQueue.isEmpty()) {
                queue.offer(tempQueue.poll());
            }
        }

        // Maybe rebalance after large deletes
        if (count > totalRows / 4) {  // If we deleted more than 25%
            rebalanceQueues();
        }

        return count;
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    // Helper methods
    private List<Integer> getRandomOrder() {
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < NUM_QUEUES; i++) order.add(i);
        Collections.shuffle(order, random);
        return order;
    }

    private boolean shouldRebalance() {
        // Check if queues are too unbalanced
        int minSize = Integer.MAX_VALUE;
        int maxSize = 0;
        for (Queue<Map<String, String>> queue : queues) {
            int size = queue.size();
            minSize = Math.min(minSize, size);
            maxSize = Math.max(maxSize, size);
        }

        // Rebalance if difference is too large
        return maxSize > minSize * 2;
    }

    private void rebalanceQueues() {
        // Collect all rows
        List<Map<String, String>> allRows = new ArrayList<>();
        for (Queue<Map<String, String>> queue : queues) {
            while (!queue.isEmpty()) {
                allRows.add(queue.poll());
            }
        }

        // Shuffle
        Collections.shuffle(allRows, random);

        // Redistribute evenly
        int targetSize = allRows.size() / NUM_QUEUES;
        int currentQueue = 0;
        for (Map<String, String> row : allRows) {
            queues.get(currentQueue).offer(row);
            if (queues.get(currentQueue).size() >= targetSize && currentQueue < NUM_QUEUES - 1) {
                currentQueue++;
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