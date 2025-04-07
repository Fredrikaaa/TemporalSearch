package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.query.model.QueryResultMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Condition;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
            NotExecutor,
            PosExecutor,
            TemporalExecutor {
    
    /**
     * Executes a specific condition type against the appropriate indexes with a specified granularity window size
     *
     * @param condition The condition to execute
     * @param indexes Map of index name to IndexAccess
     * @param granularity Whether to return document or sentence level matches
     * @param granularitySize Window size for sentence granularity (0 = same sentence only, 1 = adjacent sentences, etc.)
     * @return Set of matches at the specified granularity level
     * @throws QueryExecutionException if execution fails
     */
    Set<QueryResultMatch> execute(T condition, Map<String, IndexAccess> indexes,
                         Query.Granularity granularity,
                         int granularitySize)
        throws QueryExecutionException;
} 