package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import java.util.Random;

/**
 * Represents a complex SELECT operation with multiple conditions
 */
public class ComplexSelectOperation extends DatabaseOperation {
    public ComplexSelectOperation(String tablePrefix, Random random) {
        super(tablePrefix, random);
    }

    @Override
    public String getType() {
        return "COMPLEX_SELECT";
    }

    @Override
    public void execute(Engine engine) {
        int type = random.nextInt(3);
        String query = switch(type) {
            case 0 -> generateAgeRangeQuery();
            case 1 -> generatePriceCategoryQuery();
            case 2 -> generateOrderSizeQuery();
            default -> generateAgeRangeQuery();
        };
        engine.executeSQL(query);
    }

    private String generateAgeRangeQuery() {
        int minAge = 20 + (random.nextInt(4) * 10);
        int maxAge = minAge + 10;
        String city = getRandomCity();
        return String.format("SELECT * FROM %susers WHERE age >= %d AND age <= %d AND city = '%s'",
                tablePrefix, minAge, maxAge, city);
    }

    private String generatePriceCategoryQuery() {
        double minPrice = 100 * (1 + random.nextInt(10));
        String category = getRandomCategory();
        return String.format("SELECT * FROM %sproducts WHERE price >= %.2f AND category = '%s'",
                tablePrefix, minPrice, category);
    }

    private String generateOrderSizeQuery() {
        int minQuantity = 5 * (1 + random.nextInt(10));
        int maxQuantity = minQuantity + 20;
        return String.format("SELECT * FROM %sorders WHERE quantity >= %d AND quantity <= %d",
                tablePrefix, minQuantity, maxQuantity);
    }

    private String getRandomCity() {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Boston", "Miami"};
        return cities[random.nextInt(cities.length)];
    }

    private String getRandomCategory() {
        String[] categories = {"Electronics", "Clothing", "Books", "Home", "Sports"};
        return categories[random.nextInt(categories.length)];
    }
}