package edu.smu.smusql.evaluation.operations;

import edu.smu.smusql.Engine;
import java.util.Random;

/**
 * Base class for all database operations
 */
public abstract class DatabaseOperation {
    protected final String tablePrefix;
    protected final Random random;

    public DatabaseOperation(String tablePrefix, Random random) {
        this.tablePrefix = tablePrefix;
        this.random = random;
    }

    public abstract String getType();
    public abstract void execute(Engine engine);
}