package edu.smu.smusql.evaluation.generators;

import edu.smu.smusql.evaluation.metrics.DataType;
import java.util.Random;

public class UpdateQueryGenerator implements QueryGenerator {
    private final Random random;
    private final ValueGenerator valueGenerator;

    public UpdateQueryGenerator() {
        this.random = new Random();
        this.valueGenerator = new ValueGenerator();
    }

    @Override
    public String generateQuery(String tableName, DataType dataType) {
        if (random.nextDouble() < 0.3) { // 30% chance of complex query
            return generateComplexQuery(tableName, dataType);
        }
        return generateSimpleQuery(tableName, dataType);
    }

    private String generateSimpleQuery(String tableName, DataType dataType) {
        String newValue = valueGenerator.generateValue(dataType, random.nextInt());
        return String.format("UPDATE %s SET value = %s WHERE id = %d",
                tableName, newValue, random.nextInt(1000));
    }

    private String generateComplexQuery(String tableName, DataType dataType) {
        String newValue = valueGenerator.generateValue(dataType, random.nextInt());
        return switch (dataType) {
            case SMALL_STRING, MEDIUM_STRING, LARGE_STRING ->
                    String.format("UPDATE %s SET value = %s WHERE value = '%s'",
                            tableName, newValue, valueGenerator.generateValue(dataType, random.nextInt()).replace("'", ""));
            case SMALL_INTEGER, LARGE_INTEGER, DECIMAL ->
                    String.format("UPDATE %s SET value = %s WHERE value >= %d AND value <= %d",
                            tableName, newValue, dataType.getMin(), dataType.getMin() + (dataType.getMax() - dataType.getMin()) / 2);
            case BOOLEAN ->
                    String.format("UPDATE %s SET value = %s WHERE value = %s",
                            tableName, newValue, random.nextBoolean());
            case DATE ->
                    String.format("UPDATE %s SET value = %s WHERE value >= '%s'",
                            tableName, newValue, valueGenerator.generateDate(random.nextInt()));
        };
    }
}
