package com.example.query.executor;

import com.example.core.IndexAccessInterface;
import com.example.query.model.Query;
import com.example.query.model.condition.Condition;
import com.example.query.executor.QueryResult;

import java.util.Map;

/**
 * Interface for executing conditions against indexes.
 * Each condition type has a corresponding executor implementation.
 *
 * @param <T> The specific condition type this executor handles
 * @see com.example.query.model.condition.Condition
 */
public sealed interface ConditionExecutor<T extends Condition> 
    permits ContainsExecutor,
            DependencyExecutor,
            LogicalExecutor,
            NerExecutor,
            PosExecutor,
            NotExecutor,
            TemporalExecutor {
    
    /**
     * Executes a specific condition type against the appropriate indexes with a specified granularity window size
     *
     * @param condition The condition to execute
     * @param indexes Map of index name to IndexAccessInterface
     * @param granularity Whether to return document or sentence level matches
     * @param granularitySize Window size for sentence granularity (0 = same sentence only, 1 = adjacent sentences, etc.)
     * @param corpusName The name of the corpus being queried
     * @return QueryResult containing MatchDetail objects representing the matches.
     * @throws QueryExecutionException if execution fails
     */
    QueryResult execute(T condition, Map<String, IndexAccessInterface> indexes,
                         Query.Granularity granularity,
                         int granularitySize,
                         String corpusName)
        throws QueryExecutionException;
} 