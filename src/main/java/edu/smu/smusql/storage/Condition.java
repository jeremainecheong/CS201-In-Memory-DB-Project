package edu.smu.smusql.storage;

public class Condition {
    public final String type;     // null for condition, "AND"/"OR" for connectors
    public final String column;   // column name
    public final String operator; // =, >, <, etc.
    public final String value;    // value to compare

    public Condition(String[] parts) {
        this.type = parts[0];     // null or AND/OR
        this.column = parts[1];   // column name
        this.operator = parts[2]; // operator
        this.value = parts[3];    // value
    }
}