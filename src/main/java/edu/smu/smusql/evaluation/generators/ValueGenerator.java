package edu.smu.smusql.evaluation.generators;

import edu.smu.smusql.evaluation.metrics.DataType;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;

public class ValueGenerator {
    private final Random random;

    public ValueGenerator() {
        this.random = new Random();
    }

    public String generateValue(DataType type, int seed) {
        return switch (type) {
            case SMALL_STRING, MEDIUM_STRING, LARGE_STRING -> 
                "'" + generateString(type.getMin(), type.getMax(), seed) + "'";
            case SMALL_INTEGER, LARGE_INTEGER -> 
                String.valueOf(random.nextInt(type.getMax() - type.getMin()) + type.getMin());
            case DECIMAL -> 
                String.format("%.2f", random.nextDouble() * type.getMax());
            case BOOLEAN -> 
                String.valueOf(random.nextBoolean());
            case DATE -> 
                "'" + generateDate(seed) + "'";
        };
    }

    private String generateString(int minLength, int maxLength, int seed) {
        int length = random.nextInt(maxLength - minLength) + minLength;
        Random seededRandom = new Random(seed);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) (seededRandom.nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    private String generateDate(int seed) {
        Random seededRandom = new Random(seed);
        Instant now = Instant.now();
        long daysToSubtract = seededRandom.nextInt(365 * 5);
        return now.minus(daysToSubtract, ChronoUnit.DAYS).toString();
    }
}
