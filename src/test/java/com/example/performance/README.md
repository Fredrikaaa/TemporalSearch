# Tablesaw Query Performance Tests

This directory contains performance tests for evaluating Tablesaw as a potential replacement for the current data structures used in query processing.

## Overview

The `TablesawQueryPerformanceTest` class compares the performance of our current `Set<DocSentenceMatch>` approach with a Tablesaw-based implementation for typical query operations:

1. Filtering by document ID
2. Filtering by source
3. Intersection operations (AND conditions)
4. Grouping by document

It also compares memory usage between both approaches.

## Running the Tests

The tests are disabled by default as they can take significant time to run with large datasets. To run them:

1. Remove or comment out the `@Disabled` annotation in `TablesawQueryPerformanceTest.java`
2. Run with Maven:

```bash
# Run with enhanced logging to see performance results
mvn test -Dtest=TablesawQueryPerformanceTest -Dorg.slf4j.simpleLogger.defaultLogLevel=INFO

# You can run with specific dataset sizes by modifying the @ValueSource annotation
# Current options: 100,000, 1,000,000, and 5,000,000 rows
```

## Interpreting Results

The tests output timing information for each operation using both implementations:

```
Benchmarking: Filter by documentId
Filter by documentId - Set: X ms, Tablesaw: Y ms

Benchmarking: Filter by source
Filter by source - Set: X ms, Tablesaw: Y ms

Benchmarking: Join/Intersection operation
Join/Intersection operation - Set: X ms, Tablesaw: Y ms

Benchmarking: Group by document
Group by document - Set: X ms, Tablesaw: Y ms

Memory usage comparison:
Set<DocSentenceMatch>: X MB
Tablesaw Table: Y MB
```

## Implementation Details

The test creates a synthetic dataset of `DocSentenceMatch` objects with various attributes that mimic real-world data:

- Document IDs
- Sentence IDs
- Positions with start/end offsets
- Source identifiers
- Variable bindings

For the Tablesaw implementation, the test maps this object model to a columnar representation with appropriate data types.

## Considerations for Adoption

When evaluating the results, consider:

1. **Performance Tradeoffs**: Some operations may be faster with Tablesaw, while others might be slower. Consider which operations are most critical for your use case.

2. **Memory Usage**: Tablesaw's columnar storage may use memory differently than the current approach. This can impact scalability with very large result sets.

3. **API Integration**: Adopting Tablesaw would require adapting the query execution flow to work with Tablesaw's API rather than directly with Java collections.

4. **Additional Features**: Tablesaw provides additional capabilities like aggregation, pivoting, and visualization that could be useful beyond basic query operations.

## Next Steps

Based on the test results:

1. If Tablesaw shows good performance for most operations:

   - Consider creating a prototype implementation of `QueryExecutor` that uses Tablesaw
   - Test with real-world queries and datasets

2. If Tablesaw shows mixed or poor performance:
   - Identify specific operations that are bottlenecks
   - Consider hybrid approaches or optimizations to the current implementation
