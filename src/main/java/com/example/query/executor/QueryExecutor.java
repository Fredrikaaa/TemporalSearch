package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Logical.LogicalOperator;
import com.example.query.model.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Executes queries against the provided indexes.
 * Responsible for coordinating the execution of all conditions in a query
 * and combining their results according to the query's logical structure.
 */
public class QueryExecutor {
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
    
    private final ConditionExecutorFactory executorFactory;
    private final VariableBindings variableBindings;
    
    /**
     * Creates a new QueryExecutor with the provided executor factory.
     *
     * @param executorFactory Factory for creating condition executors
     */
    public QueryExecutor(ConditionExecutorFactory executorFactory) {
        this.executorFactory = executorFactory;
        this.variableBindings = new VariableBindings();
    }
    
    /**
     * Executes a query against the provided indexes
     *
     * @param query The query to execute
     * @param indexes Map of index name to IndexAccess
     * @return Set of matches (document or sentence level based on query granularity)
     * @throws QueryExecutionException if execution fails
     */
    public Set<DocSentenceMatch> execute(Query query, Map<String, IndexAccess> indexes) 
            throws QueryExecutionException {
        logger.debug("Executing query: {}", query);
        
        // Clear variable bindings from previous executions
        variableBindings.clear();
        
        // Set granularity for execution
        Query.Granularity granularity = query.granularity();
        int granularitySize = query.granularitySize().orElse(1);
        
        // Validate granularity size
        if (granularitySize < 0 || granularitySize > 10) {
            throw new IllegalArgumentException("Granularity size must be between 0 and 10, got: " + granularitySize);
        }
        
        // Get all conditions from the query
        List<Condition> conditions = query.conditions();
        
        if (conditions.isEmpty()) {
            logger.debug("Query has no conditions, returning empty result set");
            return new HashSet<>();
        }
        
        // If there's only one condition, execute it directly
        if (conditions.size() == 1) {
            return executeCondition(conditions.get(0), indexes, granularity, granularitySize);
        }
        
        // If there are multiple conditions, create a logical AND condition and execute it
        Logical andCondition = new Logical(LogicalOperator.AND, conditions);
        return executeCondition(andCondition, indexes, granularity, granularitySize);
    }
    
    /**
     * Executes a single condition against the indexes.
     *
     * @param condition The condition to execute
     * @param indexes Map of index name to IndexAccess
     * @param granularity The query granularity
     * @param granularitySize The window size for sentence granularity
     * @return Set of matches at the specified granularity level
     * @throws QueryExecutionException if execution fails
     */
    @SuppressWarnings("unchecked")
    private Set<DocSentenceMatch> executeCondition(
            Condition condition,
            Map<String, IndexAccess> indexes,
            Query.Granularity granularity,
            int granularitySize) 
            throws QueryExecutionException {
        logger.debug("Executing condition: {} with granularity: {} and size: {}", 
                condition, granularity, granularitySize);
        
        try {
            // Get the appropriate executor for this condition type
            ConditionExecutor executor = executorFactory.getExecutor(condition);
            
            // Execute the condition
            Set<DocSentenceMatch> results = executor.execute(
                    condition, indexes, variableBindings, granularity, granularitySize);
            logger.debug("Condition {} matched {} {}", 
                    condition, 
                    results.size(),
                    granularity == Query.Granularity.DOCUMENT ? "documents" : "sentences");
            
            return results;
        } catch (IllegalArgumentException e) {
            throw new QueryExecutionException(
                "No executor found for condition type: " + condition.getClass().getName(),
                condition.toString(),
                QueryExecutionException.ErrorType.UNSUPPORTED_OPERATION
            );
        } catch (Exception e) {
            throw new QueryExecutionException(
                "Error executing condition: " + e.getMessage() + " (" + e.getClass().getSimpleName() + ")",
                e,
                condition.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Gets the variable bindings captured during query execution
     *
     * @return Variable bindings object containing all bound variables
     */
    public VariableBindings getVariableBindings() {
        return variableBindings;
    }
    
    /**
     * Converts a set of DocSentenceMatch objects to a set of document IDs.
     * This is used for backward compatibility with code that expects document IDs.
     *
     * @param matches Set of DocSentenceMatch objects
     * @return Set of document IDs
     */
    public Set<Integer> getDocumentIds(Set<DocSentenceMatch> matches) {
        return matches.stream()
                .map(DocSentenceMatch::documentId)
                .collect(Collectors.toSet());
    }
} 