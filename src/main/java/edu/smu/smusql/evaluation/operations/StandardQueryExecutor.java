package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import edu.smu.smusql.evaluation.metrics.TestResults;
import edu.smu.smusql.evaluation.metrics.DataType;
import edu.smu.smusql.evaluation.generators.*;
import java.util.Random;

public class StandardQueryExecutor implements QueryExecutor {
    private final Random random;
    private final ValueGenerator valueGenerator;
    private final SelectQueryGenerator selectGenerator;
    private final UpdateQueryGenerator updateGenerator;
    private final DeleteQueryGenerator deleteGenerator;

    public StandardQueryExecutor() {
        this.random = new Random();
        this.valueGenerator = new ValueGenerator();
        this.selectGenerator = new SelectQueryGenerator();
        this.updateGenerator = new UpdateQueryGenerator();
        this.deleteGenerator = new DeleteQueryGenerator();
    }

    @Override
    public void executeQuery(String tableName, DataType dataType, Engine dbEngine, TestResults results) {
        double op = random.nextDouble();
        long start = System.nanoTime();

        if (op < 0.4) { // 40% SELECT
            String query = selectGenerator.generateQuery(tableName, dataType);
            dbEngine.executeSQL(query);
            results.recordLatency("SELECT", System.nanoTime() - start);
        } else if (op < 0.7) { // 30% UPDATE
            String query = updateGenerator.generateQuery(tableName, dataType);
            dbEngine.executeSQL(query);
            results.recordLatency("UPDATE", System.nanoTime() - start);
        } else if (op < 0.9) { // 20% INSERT
            int id = 1000 + random.nextInt(9000);
            String value = valueGenerator.generateValue(dataType, id);
            String query = String.format("INSERT INTO %s VALUES (%d, %s)",
                    tableName, id, value);
            dbEngine.executeSQL(query);
            results.recordLatency("INSERT", System.nanoTime() - start);
        } else { // 10% DELETE
            String query = deleteGenerator.generateQuery(tableName, dataType);
            dbEngine.executeSQL(query);
            results.recordLatency("DELETE", System.nanoTime() - start);
        }
    }
}
