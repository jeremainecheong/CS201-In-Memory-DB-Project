# CS201 In-Memory Database Project

## Specialized Table Implementations

This project implements several specialized table storage strategies, each optimized for different database usage patterns. The evaluation command `evaluate` in the application runs a comprehensive test suite that demonstrates how each implementation excels in specific scenarios.

### BackwardsStackTable
**Optimized for**: LIFO (Last-In-First-Out) access patterns
- Excels during the initial data population phase where recent inserts are immediately queried
- Shows superior performance in the complex query phase when accessing recently inserted data
- Particularly efficient when:
  * Running recent analytics (e.g., "SELECT * FROM users WHERE id > X" with high X values)
  * Processing log data where newest entries are most important
  * Handling undo/redo operations

### LeakyBucketTable
**Optimized for**: Memory-constrained environments
- Performs best during the large-scale data insertion phase
- Shows efficiency when the dataset size exceeds the main bucket capacity (1000 rows)
- Particularly efficient when:
  * Running mixed queries on large datasets
  * Dealing with memory pressure
  * Handling hot/cold data separation automatically

### PingPongTable
**Optimized for**: Temporal locality in data access
- Excels during repeated access to the same subset of data
- Shows superior performance in the complex query phase with similar WHERE conditions
- Particularly efficient when:
  * Running repeated analytics on the same data subset
  * Processing time-series data with focus on recent periods
  * Handling session-based data access

### RandomQueueTable
**Optimized for**: Distributed load and concurrent access
- Performs best during the mixed operation phase with random access patterns
- Shows balanced performance across all operations
- Particularly efficient when:
  * Running mixed workloads with unpredictable patterns
  * Handling concurrent operations
  * Dealing with evenly distributed access patterns

## Performance Analysis

When running the evaluation command, observe how each implementation performs differently in various phases:

1. **Initial Population Phase**
   - BackwardsStackTable: Fast inserts with immediate access
   - LeakyBucket: Efficient memory management as data grows
   - PingPong: Builds up active/inactive sets
   - RandomQueue: Distributes load across queues

2. **Mixed Operation Phase**
   - BackwardsStackTable: Fast on recent data, slower on older
   - LeakyBucket: Consistent performance with automatic data tiering
   - PingPong: Adapts to access patterns
   - RandomQueue: Maintains steady performance

3. **Complex Query Phase**
   - BackwardsStackTable: Excels when conditions match recent data
   - LeakyBucket: Efficient when queried data is in main bucket
   - PingPong: Superior when repeatedly accessing same data subset
   - RandomQueue: Consistent performance regardless of pattern

## Interpreting Results

The TableMonitor provides insights into each implementation's performance. Look for:

1. **BackwardsStackTable**
   - Fast response times for recent data operations
   - Increasing latency for older data access
   - Best performance in recent data heavy workloads

2. **LeakyBucketTable**
   - Consistent performance despite growing data size
   - Occasional spikes during bucket rebalancing
   - Efficient memory utilization

3. **PingPongTable**
   - Improving performance on frequently accessed data
   - Periodic overhead during list swapping
   - Adaptation to access patterns

4. **RandomQueueTable**
   - Even distribution of operation times
   - No significant performance degradation under load
   - Balanced overall performance

## Usage Recommendations

Choose the appropriate implementation based on your workload:

- Use **BackwardsStackTable** when recent data access is priority
- Use **LeakyBucketTable** when dealing with memory constraints
- Use **PingPongTable** when access patterns have temporal locality
- Use **RandomQueueTable** when workload patterns are unpredictable

The evaluation results will show how each implementation handles different phases of the test suite, highlighting their strengths in their optimized scenarios.
