package edu.smu.smusql.evaluation.utils;

import java.util.*;

/**
 * Utility class for calculating various statistical measures
 */
public class StatisticsCalculator {
    /**
     * Calculates the mean of an array of values
     */
    public static double calculateMean(double[] values) {
        return Arrays.stream(values).average().orElse(0.0);
    }

    /**
     * Calculates the standard deviation of an array of values
     */
    public static double calculateStdDev(double[] values) {
        double mean = calculateMean(values);
        double variance = Arrays.stream(values)
                .map(v -> Math.pow(v - mean, 2))
                .average()
                .orElse(0.0);
        return Math.sqrt(variance);
    }

    /**
     * Calculates a percentile value from a list of numbers
     */
    public static double calculatePercentile(List<Double> values, double percentile) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }

        List<Double> sortedValues = new ArrayList<>(values);
        Collections.sort(sortedValues);

        int index = (int) Math.ceil(percentile / 100.0 * sortedValues.size()) - 1;
        index = Math.max(0, Math.min(index, sortedValues.size() - 1));

        return sortedValues.get(index);
    }

    /**
     * Calculates confidence interval for a set of measurements
     * @param values The measurements
     * @param confidenceLevel The confidence level (e.g., 0.95 for 95% confidence)
     * @return An array containing [lower bound, upper bound]
     */
    public static double[] calculateConfidenceInterval(double[] values, double confidenceLevel) {
        double mean = calculateMean(values);
        double stdDev = calculateStdDev(values);
        double n = values.length;

        // Using t-distribution for small sample sizes
        // This is a simplified approximation
        double criticalValue = 1.96; // For 95% confidence level
        if (confidenceLevel == 0.99) {
            criticalValue = 2.576;
        } else if (confidenceLevel == 0.90) {
            criticalValue = 1.645;
        }

        double margin = criticalValue * (stdDev / Math.sqrt(n));

        return new double[] {
                mean - margin,
                mean + margin
        };
    }
}
