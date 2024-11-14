package edu.smu.smusql.storage;

import java.util.List;
import java.util.Map;

/**
 * Table interface defines the contract for table operations.
 */
public interface Table {
    /**
     * Retrieves the list of column names.
     *
     * @return List of column names.
     */
    List<String> getColumns();

    /**
     * Inserts a new row into the table.
     *
     * @param values List of string values corresponding to the table's columns.
     */
    void insert(List<String> values);

    /**
     * Selects rows based on specified conditions.
     *
     * @param conditions List of conditions, each represented as a String array:
     *                   [column, operator, value]
     * @return List of rows matching the conditions.
     */
    List<Map<String, String>> select(List<String[]> conditions);

    /**
     * Updates rows based on specified conditions.
     *
     * @param column     The column to update.
     * @param newValue   The new value for the specified column.
     * @param conditions List of conditions to identify which rows to update.
     * @return The number of rows updated.
     */
    int update(String column, String newValue, List<String[]> conditions);

    /**
     * Deletes rows based on specified conditions.
     *
     * @param conditions List of conditions to identify which rows to delete.
     * @return The number of rows deleted.
     */
    int delete(List<String[]> conditions);
}
