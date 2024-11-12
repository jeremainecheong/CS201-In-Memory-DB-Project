package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import java.util.Random;

/**
 * Represents a SELECT operation with WHERE clause
 */
public class FilteredSelectOperation extends DatabaseOperation {
    public FilteredSelectOperation(String tablePrefix, Random random) {
        super(tablePrefix, random);
    }

    @Override
    public String getType() {
        return "FILTERED_SELECT";
    }

    @Override
    public void execute(Engine engine) {
        int type = random.nextInt(3);
        String query = switch(type) {
            case 0 -> generateAgeFilter();
            case 1 -> generatePriceFilter();
            case 2 -> generateQuantityFilter();
            default -> generateAgeFilter();
        };
        engine.executeSQL(query);
    }

    private String generateAgeFilter() {
        int age = 20 + random.nextInt(40);
        return String.format("SELECT * FROM %susers WHERE age > %d", tablePrefix, age);
    }

    private String generatePriceFilter() {
        double price = 100 + random.nextDouble() * 900;
        return String.format("SELECT * FROM %sproducts WHERE price > %.2f", tablePrefix, price);
    }

    private String generateQuantityFilter() {
        int quantity = 1 + random.nextInt(50);
        return String.format("SELECT * FROM %sorders WHERE quantity > %d", tablePrefix, quantity);
    }
}
