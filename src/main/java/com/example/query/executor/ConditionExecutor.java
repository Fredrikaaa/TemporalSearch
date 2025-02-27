package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.query.model.Condition;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;

import java.util.Map;
import java.util.Set;

/**
 * Interface for executing specific condition types against the appropriate indexes.
 * Each condition type (CONTAINS, NER, etc.) will have a dedicated executor implementation.
 *
 * @param <T> The specific condition type this executor handles
 */
public interface ConditionExecutor<T extends Condition> {
    /**
     * Executes a specific condition type against the appropriate indexes
     *
     * @param condition The condition to execute
     * @param indexes Map of index name to IndexAccess
     * @param variableBindings Current variable bindings to update
     * @param granularity Whether to return document or sentence level matches
     * @return Set of matches at the specified granularity level
     * @throws QueryExecutionException if execution fails
     */
    Set<DocSentenceMatch> execute(T condition, Map<String, IndexAccess> indexes,
                         VariableBindings variableBindings, Query.Granularity granularity)
        throws QueryExecutionException;
} 