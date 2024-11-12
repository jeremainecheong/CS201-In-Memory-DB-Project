package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import edu.smu.smusql.evaluation.generators.DataGenerator;
import java.util.Random;
import java.util.Map;

/**
 * Represents an INSERT operation
 */
public class InsertOperation extends DatabaseOperation {
    private final DataGenerator dataGenerator;

    public InsertOperation(String tablePrefix, Random random, DataGenerator dataGenerator) {
        super(tablePrefix, random);
        this.dataGenerator = dataGenerator;
    }

    @Override
    public String getType() {
        return "INSERT";
    }

    @Override
    public void execute(Engine engine) {
        int type = random.nextInt(3);
        int id = 10000 + random.nextInt(90000);
        Map<String, Object> data;
        String table;

        switch(type) {
            case 0:
                data = dataGenerator.generateUserData(id);
                table = "users";
                break;
            case 1:
                data = dataGenerator.generateProductData(id);
                table = "products";
                break;
            default:
                data = dataGenerator.generateOrderData(id);
                table = "orders";
                break;
        }

        String query = generateInsertQuery(tablePrefix + table, data);
        engine.executeSQL(query);
    }

    private String generateInsertQuery(String tableName, Map<String, Object> data) {
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(tableName).append(" VALUES (");

        boolean first = true;
        for (Object value : data.values()) {
            if (!first) {
                query.append(", ");
            }
            if (value instanceof String) {
                query.append("'").append(value).append("'");
            } else {
                query.append(value);
            }
            first = false;
        }
        query.append(")");

        return query.toString();
    }
}