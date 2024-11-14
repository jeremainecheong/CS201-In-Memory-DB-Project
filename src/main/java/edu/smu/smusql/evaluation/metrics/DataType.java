package edu.smu.smusql.evaluation.metrics;

public enum DataType {
    SMALL_STRING(1, 10, "STRING"),
    MEDIUM_STRING(50, 100, "STRING"),
    LARGE_STRING(200, 500, "STRING"),
    SMALL_INTEGER(0, 100, "INTEGER"),
    LARGE_INTEGER(10000, 1000000, "INTEGER"),
    DECIMAL(0, 10000, "DECIMAL"),
    BOOLEAN(0, 1, "BOOLEAN"),
    DATE(0, 0, "DATE");

    private final int min;
    private final int max;
    private final String category;

    DataType(int min, int max, String category) {
        this.min = min;
        this.max = max;
        this.category = category;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public String getCategory() {
        return category;
    }
}
