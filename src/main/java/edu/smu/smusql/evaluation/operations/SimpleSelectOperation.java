package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import java.util.Random;

/**
 * Represents a simple SELECT operation without conditions
 */
public class SimpleSelectOperation extends DatabaseOperation {
    private static final String[] TABLES = {"users", "products", "orders"};

    public SimpleSelectOperation(String tablePrefix, Random random) {
        super(tablePrefix, random);
    }

    @Override
    public String getType() {
        return "SIMPLE_SELECT";
    }

    @Override
    public void execute(Engine engine) {
        String table = TABLES[random.nextInt(TABLES.length)];
        String query = String.format("SELECT * FROM %s%s", tablePrefix, table);
        engine.executeSQL(query);
    }
}