package edu.smu.smusql.storage;

import java.util.Objects;

/**
 * DataType encapsulates different data types (Integer, Double, String) and provides
 * a unified interface for comparison and retrieval.
 */
public class DataType implements Comparable<DataType> {
    public enum Type {
        INTEGER, DOUBLE, STRING
    }

    private final Type type;
    private Comparable<?> value;

    /**
     * Constructs a DataType instance with the specified type and value.
     *
     * @param type  The data type (INTEGER, DOUBLE, STRING).
     * @param value The value corresponding to the type.
     */
    public DataType(Type type, Comparable<?> value) {
        if (type == null) {
            throw new IllegalArgumentException("Type cannot be null.");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null.");
        }
        this.type = type;
        this.value = value;
    }

    /**
     * Factory method to create a DataType instance from a string value.
     *
     * @param value The string representation of the value.
     * @return A DataType instance with the appropriate type.
     */
    public static DataType fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Null values are not supported.");
        }

        // Attempt to parse as Integer
        try {
            Integer intValue = Integer.parseInt(value);
            return new DataType(Type.INTEGER, intValue);
        } catch (NumberFormatException e1) {
            // Attempt to parse as Double
            try {
                Double doubleValue = Double.parseDouble(value);
                return new DataType(Type.DOUBLE, doubleValue);
            } catch (NumberFormatException e2) {
                // Fallback to String
                return new DataType(Type.STRING, value);
            }
        }
    }

    public Type getType() {
        return type;
    }

    public Comparable<?> getValue() {
        return value;
    }

    public void setValue(Comparable<?> value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null.");
        }
        // Ensure the new value matches the DataType's type
        switch (this.type) {
            case INTEGER:
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("Expected Integer value.");
                }
                break;
            case DOUBLE:
                if (!(value instanceof Double)) {
                    throw new IllegalArgumentException("Expected Double value.");
                }
                break;
            case STRING:
                if (!(value instanceof String)) {
                    throw new IllegalArgumentException("Expected String value.");
                }
                break;
            default:
                throw new IllegalStateException("Unsupported DataType: " + this.type);
        }
        this.value = value;
    }

    /**
     * Compares this DataType with another for ordering.
     *
     * @param other The other DataType to compare against.
     * @return Negative integer, zero, or a positive integer as this DataType is less than,
     * equal to, or greater than the specified DataType.
     */
    @Override
    public int compareTo(DataType other) {
        if (other == null) {
            throw new NullPointerException("Cannot compare to null.");
        }
        if (this.type != other.type) {
            throw new IllegalArgumentException("Cannot compare different DataType types: "
                    + this.type + " vs " + other.type);
        }

        switch (this.type) {
            case INTEGER:
                return ((Integer) this.value).compareTo((Integer) other.value);
            case DOUBLE:
                return ((Double) this.value).compareTo((Double) other.value);
            case STRING:
                return ((String) this.value).compareTo((String) other.value);
            default:
                throw new IllegalStateException("Unsupported DataType: " + this.type);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataType dataType = (DataType) o;
        return type == dataType.type && Objects.equals(value, dataType.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
