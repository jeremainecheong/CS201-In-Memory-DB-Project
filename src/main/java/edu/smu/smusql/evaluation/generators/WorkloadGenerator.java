package edu.smu.smusql.evaluation.generators;

import edu.smu.smusql.evaluation.operations.*;
import java.util.Random;

/**
 * Generates database operations according to specified workload characteristics
 */
public class WorkloadGenerator {
    private final Random random = new Random();
    private final double readRatio;
    private final DataGenerator dataGenerator;

    /**
     * Creates a new workload generator
     * @param readRatio Ratio of read operations (0.0 to 1.0)
     */
    public WorkloadGenerator(double readRatio) {
        this.readRatio = readRatio;
        this.dataGenerator = new DataGenerator();
    }

    /**
     * Generates the next database operation
     * @param tablePrefix Prefix for table names
     * @return A database operation
     */
    public DatabaseOperation nextOperation(String tablePrefix) {
        if (random.nextDouble() < readRatio) {
            return generateReadOperation(tablePrefix);
        } else {
            return generateWriteOperation(tablePrefix);
        }
    }

    private DatabaseOperation generateReadOperation(String tablePrefix) {
        int type = random.nextInt(3);
        switch (type) {
            case 0:
                return new SimpleSelectOperation(tablePrefix, random);
            case 1:
                return new FilteredSelectOperation(tablePrefix, random);
            default:
                return new ComplexSelectOperation(tablePrefix, random);
        }
    }

    private DatabaseOperation generateWriteOperation(String tablePrefix) {
        int type = random.nextInt(3);
        switch (type) {
            case 0:
                return new InsertOperation(tablePrefix, random, dataGenerator);
            case 1:
                return new UpdateOperation(tablePrefix, random);
            default:
                return new DeleteOperation(tablePrefix, random);
        }
    }
}