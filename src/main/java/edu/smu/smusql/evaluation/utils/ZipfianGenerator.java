package edu.smu.smusql.evaluation.utils;

import java.util.Random;

/**
 * Generates numbers following a Zipfian distribution
 * Used to simulate real-world phenomena like popularity distributions
 */
public class ZipfianGenerator {
    private final double alpha;
    private final Random random;
    private final double zeta;
    private final int itemCount;

    /**
     * Creates a new Zipfian generator
     * @param alpha The skewness parameter (higher means more skewed)
     */
    public ZipfianGenerator(double alpha) {
        this(alpha, 1000); // Default to 1000 items
    }

    /**
     * Creates a new Zipfian generator with specified number of items
     * @param alpha The skewness parameter
     * @param itemCount The number of possible items
     */
    public ZipfianGenerator(double alpha, int itemCount) {
        this.alpha = alpha;
        this.itemCount = itemCount;
        this.random = new Random();
        this.zeta = calculateZeta(itemCount, alpha);
    }

    /**
     * Generates the next Zipfian number
     * @return A number between 0 and itemCount-1
     */
    public int nextInt() {
        return nextInt(itemCount);
    }

    /**
     * Generates the next Zipfian number with a specified bound
     * @param bound The upper bound (exclusive)
     * @return A number between 0 and bound-1
     */
    public int nextInt(int bound) {
        double u = random.nextDouble();
        double uz = u * zeta;

        for (int i = 1; i <= bound; i++) {
            if (uz < calculateZeta(i, alpha)) {
                return i - 1;
            }
        }
        return bound - 1;
    }

    private double calculateZeta(int n, double alpha) {
        double sum = 0.0;
        for (int i = 1; i <= n; i++) {
            sum += 1.0 / Math.pow(i, alpha);
        }
        return sum;
    }
}