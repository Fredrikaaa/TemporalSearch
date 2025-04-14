package com.example.query.executor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;

import tech.tablesaw.api.Table;

import com.example.query.executor.QueryResult;
import com.example.query.model.SubquerySpec;

/**
 * Maintains the execution context for subqueries, including
 * intermediate results as QueryResult objects.
 * This class serves as a container for subquery results during
 * the recursive execution of queries with subqueries.
 */
public class SubqueryContext {
    private final Map<String, QueryResult> queryResults;
    
    /**
     * Creates an empty subquery context.
     */
    public SubqueryContext() {
        this.queryResults = new HashMap<>();
    }
    
    /**
     * Adds the QueryResult for a subquery.
     * 
     * @param subquery The subquery specification
     * @param result The QueryResult object containing match details.
     */
    public void addQueryResult(SubquerySpec subquery, QueryResult result) {
        Objects.requireNonNull(subquery, "Subquery cannot be null");
        Objects.requireNonNull(result, "QueryResult cannot be null");
        
        queryResults.put(subquery.alias(), result);
    }
    
    /**
     * Adds a query result directly using an alias.
     * Used for storing the result of the main query part when handling joins.
     *
     * @param alias The alias to associate with the result.
     * @param result The QueryResult.
     */
    public void addQueryResult(String alias, QueryResult result) {
        Objects.requireNonNull(alias, "alias cannot be null");
        Objects.requireNonNull(result, "QueryResult cannot be null");
        if (queryResults.containsKey(alias)) {
            // Decide on behavior: overwrite or throw? For now, overwrite might be okay for implicit main query.
            System.out.println("Overwriting existing QueryResult for alias: " + alias);
        }
        queryResults.put(alias, result);
        // Note: aliasToSpecMap will not have an entry for aliases added this way.
    }
    
    /**
     * Gets the QueryResult for a subquery by its alias.
     * 
     * @param alias The subquery alias
     * @return The QueryResult object, or null if not found
     */
    public QueryResult getQueryResult(String alias) {
        return queryResults.get(alias);
    }
    
    /**
     * Checks if a subquery has results stored.
     * 
     * @param alias The subquery alias
     * @return true if the subquery has a QueryResult stored, false otherwise
     */
    public boolean hasResults(String alias) {
        return queryResults.containsKey(alias);
    }
    
    /**
     * Gets the set of all subquery aliases with results.
     * 
     * @return The set of subquery aliases that have QueryResults stored
     */
    public Set<String> getAliases() {
        return new HashSet<>(queryResults.keySet());
    }
    
    /**
     * Creates a join variable name in the format expected by the TemporalExecutor.
     * 
     * @param leftAlias The left subquery alias
     * @param rightAlias The right subquery alias
     * @return A variable name in the format "join.leftAlias.rightAlias"
     */
    public static String createJoinVariableName(String leftAlias, String rightAlias) {
        Objects.requireNonNull(leftAlias, "Left alias cannot be null");
        Objects.requireNonNull(rightAlias, "Right alias cannot be null");
        
        return "join." + leftAlias + "." + rightAlias;
    }
} 