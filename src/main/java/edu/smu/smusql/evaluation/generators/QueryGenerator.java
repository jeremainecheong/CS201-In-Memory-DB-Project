package edu.smu.smusql.evaluation.generators;

import edu.smu.smusql.evaluation.metrics.DataType;

public interface QueryGenerator {
    String generateQuery(String tableName, DataType dataType);
}
