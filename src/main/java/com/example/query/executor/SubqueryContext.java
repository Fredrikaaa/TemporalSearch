package com.example.query.executor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;

import tech.tablesaw.api.Table;

import com.example.query.binding.BindingContext;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.SubquerySpec;

/**
 * Maintains the execution context for subqueries, including
 * intermediate results in both native and Tablesaw formats.
 * This class serves as a container for subquery results during
 * the recursive execution of queries with subqueries.
 */
public class SubqueryContext {
    private final Map<String, Set<DocSentenceMatch>> nativeResults;
    private final Map<String, Table> tableResults;
    
    /**
     * Creates an empty subquery context.
     */
    public SubqueryContext() {
        this.nativeResults = new HashMap<>();
        this.tableResults = new HashMap<>();
    }
    
    /**
     * Adds the native result set for a subquery.
     * 
     * @param subquery The subquery specification
     * @param results The matching document/sentence results
     */
    public void addNativeResults(SubquerySpec subquery, Set<DocSentenceMatch> results) {
        Objects.requireNonNull(subquery, "Subquery cannot be null");
        Objects.requireNonNull(results, "Results cannot be null");
        
        nativeResults.put(subquery.alias(), results);
    }
    
    /**
     * Adds the Tablesaw table result for a subquery.
     * 
     * @param subquery The subquery specification
     * @param table The Tablesaw table containing the results
     */
    public void addTableResults(SubquerySpec subquery, Table table) {
        Objects.requireNonNull(subquery, "Subquery cannot be null");
        Objects.requireNonNull(table, "Table cannot be null");
        
        tableResults.put(subquery.alias(), table);
    }
    
    /**
     * Gets the native results for a subquery by its alias.
     * 
     * @param alias The subquery alias
     * @return The set of document/sentence matches, or null if not found
     */
    public Set<DocSentenceMatch> getNativeResults(String alias) {
        return nativeResults.get(alias);
    }
    
    /**
     * Gets the table results for a subquery by its alias.
     * 
     * @param alias The subquery alias
     * @return The Tablesaw table, or null if not found
     */
    public Table getTableResults(String alias) {
        return tableResults.get(alias);
    }
    
    /**
     * Checks if a subquery has results.
     * 
     * @param alias The subquery alias
     * @return true if the subquery has native results or table results, false otherwise
     */
    public boolean hasResults(String alias) {
        return nativeResults.containsKey(alias) || tableResults.containsKey(alias);
    }
    
    /**
     * Gets the set of all subquery aliases with results.
     * 
     * @return The set of subquery aliases that have either native or table results
     */
    public Set<String> getAliases() {
        Set<String> aliases = new HashSet<>(nativeResults.keySet());
        aliases.addAll(tableResults.keySet());
        return aliases;
    }
    
    /**
     * Binds this subquery context to a variable in the binding context.
     * This makes the subquery results available for temporal join conditions.
     * 
     * @param variableName The variable name to bind to
     * @param bindingContext The binding context to update
     */
    public void bindToContext(String variableName, BindingContext bindingContext) {
        Objects.requireNonNull(variableName, "Variable name cannot be null");
        Objects.requireNonNull(bindingContext, "Binding context cannot be null");
        
        bindingContext.bindValue(variableName, this);
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