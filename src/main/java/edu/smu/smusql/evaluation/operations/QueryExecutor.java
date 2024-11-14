package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import edu.smu.smusql.evaluation.metrics.TestResults;
import edu.smu.smusql.evaluation.metrics.DataType;

public interface QueryExecutor {
    void executeQuery(String tableName, DataType dataType, Engine dbEngine, TestResults results);
}
