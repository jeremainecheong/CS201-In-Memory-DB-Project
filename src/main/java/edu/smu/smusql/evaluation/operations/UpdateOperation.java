package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import java.util.Random;

/**
 * Represents an UPDATE operation
 */
public class UpdateOperation extends DatabaseOperation {
    public UpdateOperation(String tablePrefix, Random random) {
        super(tablePrefix, random);
    }

    @Override
    public String getType() {
        return "UPDATE";
    }

    @Override
    public void execute(Engine engine) {
        int type = random.nextInt(3);
        String query = switch(type) {
            case 0 -> generateUserUpdate();
            case 1 -> generateProductUpdate();
            case 2 -> generateOrderUpdate();
            default -> generateUserUpdate();
        };
        engine.executeSQL(query);
    }

    private String generateUserUpdate() {
        int id = random.nextInt(10000);
        int newAge = 20 + random.nextInt(40);
        return String.format("UPDATE %susers SET age = %d WHERE id = %d",
                tablePrefix, newAge, id);
    }

    private String generateProductUpdate() {
        int id = random.nextInt(10000);
        double newPrice = 100 + random.nextDouble() * 900;
        return String.format("UPDATE %sproducts SET price = %.2f WHERE id = %d",
                tablePrefix, newPrice, id);
    }

    private String generateOrderUpdate() {
        int id = random.nextInt(10000);
        int newQuantity = 1 + random.nextInt(100);
        return String.format("UPDATE %sorders SET quantity = %d WHERE id = %d",
                tablePrefix, newQuantity, id);
    }
}