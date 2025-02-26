package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.query.model.Condition;
import com.example.query.model.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
     * @return Set of document IDs matching the query
     * @throws QueryExecutionException if execution fails
     */
    public Set<Integer> execute(Query query, Map<String, IndexAccess> indexes) 
            throws QueryExecutionException {
        logger.debug("Executing query: {}", query);
        
        // Clear variable bindings from previous executions
        variableBindings.clear();
        
        // Get all conditions from the query
        Set<Integer> results = new HashSet<>();
        boolean firstCondition = true;
        
        // Execute each condition and combine results
        for (Condition condition : query.getConditions()) {
            Set<Integer> conditionResults = executeCondition(condition, indexes);
            
            if (firstCondition) {
                results.addAll(conditionResults);
                firstCondition = false;
            } else {
                // Implement AND semantics between conditions
                results.retainAll(conditionResults);
            }
            
            if (results.isEmpty()) {
                // Short-circuit if no results
                logger.debug("No results after condition {}, short-circuiting", condition);
                break;
            }
        }
        
        logger.debug("Query execution complete, found {} matching documents", results.size());
        return results;
    }
    
    /**
     * Executes a single condition against the indexes.
     *
     * @param condition The condition to execute
     * @param indexes Map of index name to IndexAccess
     * @return Set of document IDs matching the condition
     * @throws QueryExecutionException if execution fails
     */
    @SuppressWarnings("unchecked")
    private Set<Integer> executeCondition(Condition condition, Map<String, IndexAccess> indexes) 
            throws QueryExecutionException {
        logger.debug("Executing condition: {}", condition);
        
        try {
            // Get the appropriate executor for this condition type
            ConditionExecutor executor = executorFactory.getExecutor(condition);
            
            // Execute the condition
            Set<Integer> results = executor.execute(condition, indexes, variableBindings);
            logger.debug("Condition {} matched {} documents", condition, results.size());
            
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
} 