package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import edu.smu.smusql.evaluation.metrics.TestResults;
import edu.smu.smusql.evaluation.metrics.DataType;
import edu.smu.smusql.evaluation.generators.*;

public class ComplexQueryExecutor implements QueryExecutor {
    private final SelectQueryGenerator selectGenerator;
    private final UpdateQueryGenerator updateGenerator;
    private final DeleteQueryGenerator deleteGenerator;

    public ComplexQueryExecutor() {
        this.selectGenerator = new SelectQueryGenerator();
        this.updateGenerator = new UpdateQueryGenerator();
        this.deleteGenerator = new DeleteQueryGenerator();
    }

    @Override
    public void executeQuery(String tableName, DataType dataType, Engine dbEngine, TestResults results) {
        // Complex SELECT query
        long start = System.nanoTime();
        String selectQuery = selectGenerator.generateQuery(tableName, dataType);
        dbEngine.executeSQL(selectQuery);
        results.recordLatency("COMPLEX_SELECT", System.nanoTime() - start);

        // Complex UPDATE query
        start = System.nanoTime();
        String updateQuery = updateGenerator.generateQuery(tableName, dataType);
        dbEngine.executeSQL(updateQuery);
        results.recordLatency("COMPLEX_UPDATE", System.nanoTime() - start);

        // Complex DELETE query
        start = System.nanoTime();
        String deleteQuery = deleteGenerator.generateQuery(tableName, dataType);
        dbEngine.executeSQL(deleteQuery);
        results.recordLatency("COMPLEX_DELETE", System.nanoTime() - start);
    }
}
