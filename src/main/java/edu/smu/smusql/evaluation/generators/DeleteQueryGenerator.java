package edu.smu.smusql.evaluation.generators;

import edu.smu.smusql.evaluation.metrics.DataType;
import java.util.Random;

public class DeleteQueryGenerator implements QueryGenerator {
    private final Random random;
    private final ValueGenerator valueGenerator;

    public DeleteQueryGenerator() {
        this.random = new Random();
        this.valueGenerator = new ValueGenerator();
    }

    @Override
    public String generateQuery(String tableName, DataType dataType) {
        if (random.nextDouble() < 0.3) { // 30% chance of complex query
            return generateComplexQuery(tableName, dataType);
        }
        return generateSimpleQuery(tableName);
    }

    private String generateSimpleQuery(String tableName) {
        return String.format("DELETE FROM %s WHERE id = %d",
                tableName, random.nextInt(1000));
    }

    private String generateComplexQuery(String tableName, DataType dataType) {
        return switch (dataType) {
            case SMALL_INTEGER, LARGE_INTEGER, DECIMAL ->
                String.format("DELETE FROM %s WHERE value >= %d AND value <= %d",
                        tableName, dataType.getMin(), dataType.getMin() + (dataType.getMax() - dataType.getMin()) / 2);
            case BOOLEAN, SMALL_STRING, MEDIUM_STRING, LARGE_STRING ->
                String.format("DELETE FROM %s WHERE value = %s",
                        tableName, random.nextBoolean());
            case DATE ->
                String.format("DELETE FROM %s WHERE value >= %s",
                        tableName, valueGenerator.generateValue(dataType, random.nextInt()));
        };
    }
}
