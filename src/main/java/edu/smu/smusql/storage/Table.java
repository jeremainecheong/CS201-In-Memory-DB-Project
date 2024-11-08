package edu.smu.smusql.storage;

import java.util.*;

public interface Table {
    void insert(List<String> values);
    List<Map<String, String>> select(List<String[]> conditions); // Using String[] to match Parser's condition format
    int update(String column, String value, List<String[]> conditions);
    int delete(List<String[]> conditions);
    List<String> getColumns();
}
