# smuSQL Project
Authors: Ian Hoe, Jayden Teoh, Jered Wong, Jeremaine Cheong, Jerome Wong, Keegan Ravindran

## Vision
Traditional database systems rely on highly optimized, general-purpose structures. This project reimagines their design by enhancing fundamental data structures with specialized tweaks to optimize specific query patterns, challenging conventional principles to explore impacts on scalability and performance.

## Design Overview

### Data Structures Implemented

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

## Setup and Usage

### Prerequisites
- Java JDK 17
- Apache Maven
- Python 3.x (for visualizations)
- Python packages:
  ```bash
  pip install pandas matplotlib seaborn numpy
  ```

### Building and Running
1. **Compile the project**
   ```bash
   mvn compile
   ```

2. **Run the program**
   ```bash
   mvn exec:java
   ```


### Available Commands

#### System Commands
```sql
-- Exit the program
exit

-- Run evaluation tests
evaluate

-- Display SQL command syntax 
help
```

#### SQL Commands and Implementation Selection
Tables can be created with specific implementations using prefixes. If no prefix is specified, ChunkTable is used by default.

**Implementation Prefixes:**
- No prefix or `chunk_` : Uses ChunkTable
- `forest_` : Uses ForestMapTable
- `lfu_` : Uses LFUTable

```sql
-- Using default implementation (ChunkTable)
CREATE TABLE students (id, name, age, gpa)
CREATE TABLE chunk_students (id, name, age, gpa)     -- Same as above

-- Using specific implementations
CREATE TABLE forest_records (id, name, age, gpa)     -- Uses tree-based indexing
CREATE TABLE lfu_accounts (id, name, age, gpa)       -- Uses frequency-based caching

-- Standard SQL operations work the same for all implementations
INSERT INTO students VALUES (1, John, 20, 3.5)
SELECT * FROM forest_records WHERE id < 1000
UPDATE lfu_accounts SET gpa = 4.0 WHERE id = 1
DELETE FROM students WHERE gpa < 2.0
```

#### Supported Query Conditions
- Equality: `column = value`
- Comparisons: `column < value`, `column > value`, `column <= value`, `column >= value`
- Logical operators: `AND`, `OR`

Example queries highlighting implementation strengths:
```sql
-- Sequential access (ChunkTable - default)
CREATE TABLE students (id, name, age, gpa)
SELECT * FROM students WHERE id >= 100 AND id <= 200

-- Range queries (ForestMapTable)
CREATE TABLE forest_students (id, name, age, gpa)
SELECT * FROM forest_students WHERE gpa >= 3.0 AND age < 25

-- Frequently accessed data (FrequencyTable)
CREATE TABLE lfu_students (id, name, age, gpa)
SELECT * FROM lfu_students WHERE id = 1
```

## Evaluation Framework

### Types of Tests

1. **Comprehensive Performance Tests**
   - Tests each implementation against different data types
   - Measures operation latencies
   - Analyzes memory usage
   - Generates detailed performance reports

2. **Query Pattern Testing**
   - Sequential Access: Evaluates ordered data access performance
   - Frequency-based Access: Tests real-world access patterns using Zipfian distribution
   - Range-based Access: Measures range query efficiency

3. **Scalability Tests**
   - Tests performance with increasing data sizes (100, 1000, 10000, 30000 rows)
   - Measures throughput and latency
   - Analyzes memory overhead

### Running Tests
When running `evaluate`, you'll be prompted:
```
Skip to scalability tests? (y/n)
> n    // Run comprehensive and pattern tests first
> y    // Skip to scalability tests
```

### Test Output
Results are saved in `evaluation_results/`:
- Performance Metrics:
  - `summary_statistics.csv`: Detailed performance data
  - `conditional_metrics.csv`: Query condition analysis
  - `scalability_test_results.csv`: Scalability measurements
- Access Pattern Analysis:
  - `frequency_metrics.csv`: Zipfian pattern results
  - `sequential_metrics.csv`: Sequential access data
  - `range_metrics.csv`: Range query statistics

### Generating Visualizations
Run the visualization suite:
```bash
python visualization_script.py
python scalability_visualization.py
python access_pattern_visualization.py
```

Generated visualizations include:
- Performance analysis
  - `operation_performance.png`: Operation performance heatmap
  - `memory_usage.png`: Memory usage patterns
  - `data_type_performance.png`: Type-specific performance
- Pattern analysis
  - `sequential_performance.png`: Sequential access analysis
  - `frequency_based_performance.png`: Zipfian distribution results
  - `range_query_performance.png`: Range query efficiency
- Scalability analysis
  - `scalability_trend.png`: Performance scaling
  - `memory_scaling.png`: Memory usage patterns
  - `throughput_analysis.png`: Throughput characteristics

---

*This project is part of CS201 Data Structures and Algorithms, focusing on the experimental analysis of specialized data structures in database systems.*