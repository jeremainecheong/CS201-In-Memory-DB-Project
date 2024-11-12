package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import java.util.Random;

/**
 * Represents a DELETE operation
 */
public class DeleteOperation extends DatabaseOperation {
    public DeleteOperation(String tablePrefix, Random random) {
        super(tablePrefix, random);
    }

    @Override
    public String getType() {
        return "DELETE";
    }

    @Override
    public void execute(Engine engine) {
        int type = random.nextInt(3);
        String query = switch(type) {
            case 0 -> generateUserDelete();
            case 1 -> generateProductDelete();
            case 2 -> generateOrderDelete();
            default -> generateUserDelete();
        };
        engine.executeSQL(query);
    }

    private String generateUserDelete() {
        int id = random.nextInt(10000);
        return String.format("DELETE FROM %susers WHERE id = %d", tablePrefix, id);
    }

    private String generateProductDelete() {
        int id = random.nextInt(10000);
        return String.format("DELETE FROM %sproducts WHERE id = %d", tablePrefix, id);
    }

    private String generateOrderDelete() {
        int id = random.nextInt(10000);
        return String.format("DELETE FROM %sorders WHERE id = %d", tablePrefix, id);
    }
}