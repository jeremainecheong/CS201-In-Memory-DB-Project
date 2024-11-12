package edu.smu.smusql.evaluation.generators;

import java.util.Random;

/**
 * Generates complex SQL queries for testing
 */
public class ComplexQueryGenerator {
    private final String tablePrefix;
    private final Random random;

    public ComplexQueryGenerator(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        this.random = new Random();
    }

    public String generateComplexQuery() {
        int queryType = random.nextInt(4);
        switch (queryType) {
            case 0:
                return generateAgeRangeQuery();
            case 1:
                return generatePriceRangeQuery();
            case 2:
                return generateQuantityRangeQuery();
            default:
                return generateCityAgeQuery();
        }
    }

    private String generateAgeRangeQuery() {
        int minAge = 20 + (random.nextInt(4) * 10);
        int maxAge = minAge + 10;
        return String.format("SELECT * FROM %susers WHERE age >= %d AND age <= %d",
                tablePrefix, minAge, maxAge);
    }

    private String generatePriceRangeQuery() {
        double[] priceBands = {100, 500, 1000, 5000};
        int bandIndex = random.nextInt(priceBands.length);
        double minPrice = priceBands[bandIndex];
        double maxPrice = (bandIndex < priceBands.length - 1) ?
                priceBands[bandIndex + 1] : Double.MAX_VALUE;

        return String.format("SELECT * FROM %sproducts WHERE price >= %.2f AND price <= %.2f",
                tablePrefix, minPrice, maxPrice);
    }

    private String generateQuantityRangeQuery() {
        int[] thresholds = {2, 7, 20, 50};
        int threshold = thresholds[random.nextInt(thresholds.length)];
        return String.format("SELECT * FROM %sorders WHERE quantity > %d",
                tablePrefix, threshold);
    }

    private String generateCityAgeQuery() {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Boston", "Miami"};
        String city = cities[random.nextInt(cities.length)];
        int ageThreshold = 30 + (random.nextInt(4) * 10);

        return String.format("SELECT * FROM %susers WHERE city = '%s' AND age > %d",
                tablePrefix, city, ageThreshold);
    }
}