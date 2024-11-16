# smuSQL Project
Authors: Ian Hoe, Jayden Teoh, Jered Wong, Jeremaine Cheong, Jerome Wong, Keegan Ravindran

## Vision
Traditional database systems rely on highly optimized, general-purpose structures. This project reimagines their design by enhancing fundamental data structures with specialized tweaks to optimize specific query patterns, challenging conventional principles to explore impacts on scalability and performance.

## Approach

### Data Structures Tested

1. **ForestMapTable**
   - *Hypothesis:* Multiple synchronized tree-based indexes could provide efficient querying across different columns while maintaining reasonable memory overhead
   - *Implementation:* TreeMap-based multi-index structure using bidirectional node links between column values
   - *Focus:* Range query optimization

2. **ChunkTable**
   - *Hypothesis:* Fixed-size data chunks could improve locality and sequential access performance
   - *Implementation:* Array-based storage with 128-row chunks and MRU tracking
   - *Focus:* Sequential access optimisation and memory efficiency

3. **LFUTable**
   - *Hypothesis:* Frequency-based data organization could naturally optimize for real-world access patterns
   - *Implementation:* Hybrid approach combining LFU cache with a backing store
   - *Focus:* 'Popularity bias' pattern optimization

### Getting Started

#### Prerequisites
- Java JDK 17
- Apache Maven

#### Building and Running
1. **Compile the project**
   ```bash
   mvn compile
   ```

2. **Run the program**
   ```bash
   mvn exec:java
   ```

3. **Available Commands**
   ```sql
   -- Exit the program
   exit
   
   -- Run evaluation tests
   evaluate
   
   -- Execute SQL commands
   CREATE TABLE table_name (column1, column2, ...)
   INSERT INTO table_name VALUES (value1, value2, ...)
   SELECT * FROM table_name WHERE conditions
   UPDATE table_name SET update WHERE conditions
   DELETE FROM table_name WHERE conditions
   ```

#### Running Experiments
The evaluation framework provides three types of comprehensive tests:

1. **Comprehensive Performance Tests**
   - Tests each implementation against different data types
   - Measures operation latencies
   - Analyzes memory usage
   - Generates detailed performance reports

2. **Query Pattern Testing**
   - Tests implementation behavior across different access patterns:
     
     a. **Sequential Access**
     - Evaluates performance of ordered data access
     - Tests behavior with consecutive primary key lookups
     - Measures impact of data locality
     
     b. **Frequency-based Access**
     - Uses Zipfian distribution to simulate real-world access patterns
     - Tests performance with "hot" data paths
     - Analyzes cache effectiveness
     
     c. **Range-based Access**
     - Tests performance of range queries
     - Evaluates efficiency of index structures
     - Measures impact on memory usage

3. **Scalability Tests**
   - Tests performance with increasing data sizes
   - Row counts: 100, 1000, 10000, 30000
   - Measures throughput and latency
   - Analyzes memory overhead

When running `evaluate`, you'll be prompted to choose:
```
Skip to scalability tests? (y/n)
> n    // Run comprehensive and pattern tests first
> y    // Skip to scalability tests
```

#### Output Files
Performance results are saved in the `evaluation_results` directory:
- `summary_statistics.csv`: Detailed performance metrics
- `conditional_metrics.csv`: Query condition analysis
- `scalability_test_results.csv`: Scalability measurements
- Access pattern metrics: 
  - `frequency_metrics.csv`: Zipfian access pattern results
  - `sequential_metrics.csv`: Sequential access performance
  - `range_metrics.csv`: Range query analysis

#### Generating Visualizations

##### Prerequisites
- Python 3.x
- Required Python packages:
  ```bash
  pip install pandas matplotlib seaborn numpy
  ```

##### Running the Visualization Suite
```bash
# Run all visualization scripts
python visualization_script.py
python scalability_visualization.py
python access_pattern_visualization.py
```

All generated visualizations will be saved in the `evaluation_results` directory alongside their corresponding data files.

## Results and Insights

### Structure-Specific Findings

1. **ForestMapTable**
TODO

2. **ChunkTable**
TODO

3. **LFUTable**
TODO

### Key Observations

TODO

## Conclusion

This experimental study revealed several important insights about data structure specialization in database systems:

TODO

---

*This project is part of CS201 Data Structures and Algorithms, focusing on the experimental analysis of specialized data structures in database systems.*