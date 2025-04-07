package com.example.query.executor;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.selection.Selection;

import com.example.query.model.JoinCondition;
import com.example.query.model.TemporalPredicate;
import com.example.query.result.ResultGenerationException;

/**
 * Manages and executes temporal join operations between tables.
 * Uses a strategy pattern to support multiple implementations for testing and benchmarking.
 */
public class JoinExecutor {
    private static final Logger logger = LoggerFactory.getLogger(JoinExecutor.class);
    
    // Registry of join strategies by predicate and implementation name
    private final Map<TemporalPredicate, Map<String, TemporalJoinStrategy>> strategies = new HashMap<>();
    
    // Currently selected strategy for each predicate
    private final Map<TemporalPredicate, String> activeStrategies = new HashMap<>();
    
    /**
     * Creates a JoinExecutor without any registered strategies.
     * Strategies must be registered before joins can be executed.
     */
    public JoinExecutor() {
        // Empty constructor - strategies must be registered separately
    }
    
    /**
     * Registers a join strategy for a specific temporal predicate with the default name and sets it as active.
     * This is a convenience method for simple cases where only one implementation per predicate is needed.
     * 
     * @param predicate The temporal predicate
     * @param strategy The implementation strategy
     * @return This JoinExecutor instance for method chaining
     */
    public JoinExecutor registerStrategy(TemporalPredicate predicate, TemporalJoinStrategy strategy) {
        return registerStrategy(predicate, "default", strategy, true);
    }
    
    /**
     * Registers a join strategy for a specific temporal predicate.
     * 
     * @param predicate The temporal predicate
     * @param name The name of the implementation
     * @param strategy The implementation strategy
     * @param makeActive Whether to make this the active strategy for the predicate
     * @return This JoinExecutor instance for method chaining
     */
    public JoinExecutor registerStrategy(
            TemporalPredicate predicate, 
            String name, 
            TemporalJoinStrategy strategy,
            boolean makeActive) {
        
        strategies.computeIfAbsent(predicate, k -> new HashMap<>()).put(name, strategy);
        
        if (makeActive) {
            activeStrategies.put(predicate, name);
        }
        
        return this;
    }
    
    /**
     * Sets the active strategy for a temporal predicate.
     * 
     * @param predicate The temporal predicate
     * @param name The name of the strategy to activate
     * @return This JoinExecutor instance for method chaining
     * @throws IllegalArgumentException if the strategy name is not registered
     */
    public JoinExecutor setActiveStrategy(TemporalPredicate predicate, String name) {
        if (!strategies.containsKey(predicate) || !strategies.get(predicate).containsKey(name)) {
            throw new IllegalArgumentException(
                    "Strategy '" + name + "' for predicate " + predicate + " is not registered");
        }
        
        activeStrategies.put(predicate, name);
        return this;
    }
    
    /**
     * Gets the names of all registered strategies for a predicate.
     * 
     * @param predicate The temporal predicate
     * @return A set of strategy names
     */
    public Set<String> getStrategyNames(TemporalPredicate predicate) {
        return strategies.getOrDefault(predicate, Map.of()).keySet();
    }
    
    /**
     * Gets the currently active strategy name for a predicate.
     * 
     * @param predicate The temporal predicate
     * @return The active strategy name or null if none is active
     */
    public String getActiveStrategy(TemporalPredicate predicate) {
        return activeStrategies.get(predicate);
    }
    
    /**
     * Gets a specific strategy implementation.
     * 
     * @param predicate The temporal predicate
     * @param name The name of the strategy
     * @return The strategy implementation or null if not found
     */
    public TemporalJoinStrategy getStrategy(TemporalPredicate predicate, String name) {
        return strategies.getOrDefault(predicate, Map.of()).get(name);
    }
    
    /**
     * Gets the active strategy for a predicate.
     * 
     * @param predicate The temporal predicate
     * @param joinCondition The join condition (for error reporting)
     * @return The active strategy implementation
     * @throws ResultGenerationException if no active strategy is set
     */
    public TemporalJoinStrategy getActiveStrategy(TemporalPredicate predicate, JoinCondition joinCondition)
            throws ResultGenerationException {
        String activeName = activeStrategies.get(predicate);
        
        if (activeName == null) {
            throw new ResultGenerationException(
                "No active strategy set for temporal predicate: " + predicate,
                "join_executor",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        TemporalJoinStrategy strategy = strategies.getOrDefault(predicate, Map.of()).get(activeName);
        
        if (strategy == null) {
            throw new ResultGenerationException(
                "Active strategy '" + activeName + "' not found for predicate: " + predicate,
                "join_executor",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        return strategy;
    }

    /**
     * Joins two tables based on a temporal join condition.
     * 
     * @param leftTable The left table
     * @param rightTable The right table
     * @param joinCondition The temporal join condition
     * @return A new table containing the joined results
     * @throws ResultGenerationException if join execution fails
     */
    public Table join(Table leftTable, Table rightTable, JoinCondition joinCondition) 
            throws ResultGenerationException {
        
        TemporalPredicate predicate = joinCondition.temporalPredicate();
        TemporalJoinStrategy strategy = getActiveStrategy(predicate, joinCondition);
        
        logger.debug("Executing {} join with temporal predicate {} between {}.{} and {}.{} using strategy {}",
                joinCondition.type(), predicate, 
                leftTable.name(), joinCondition.leftColumn(), 
                rightTable.name(), joinCondition.rightColumn(),
                getActiveStrategy(predicate));
        
        return strategy.execute(leftTable, rightTable, joinCondition);
    }
    
    /**
     * Benchmarks all registered strategies for a join operation.
     * 
     * @param leftTable The left table
     * @param rightTable The right table
     * @param joinCondition The join condition
     * @return A map of strategy names to execution times in milliseconds
     * @throws ResultGenerationException if any strategy execution fails
     */
    public Map<String, Long> benchmark(
            Table leftTable, Table rightTable, JoinCondition joinCondition) 
            throws ResultGenerationException {
        
        TemporalPredicate predicate = joinCondition.temporalPredicate();
        Map<String, TemporalJoinStrategy> impls = strategies.getOrDefault(predicate, Map.of());
        Map<String, Long> results = new HashMap<>();
        
        if (impls.isEmpty()) {
            logger.warn("No strategies registered for predicate {}", predicate);
            return results;
        }
        
        for (Map.Entry<String, TemporalJoinStrategy> entry : impls.entrySet()) {
            String name = entry.getKey();
            TemporalJoinStrategy strategy = entry.getValue();
            
            logger.info("Benchmarking strategy {} for predicate {}", name, predicate);
            
            // Warm-up run (ignore results)
            strategy.execute(leftTable, rightTable, joinCondition);
            
            // Timed run
            long startTime = System.nanoTime();
            strategy.execute(leftTable, rightTable, joinCondition);
            long endTime = System.nanoTime();
            
            long executionTimeMs = (endTime - startTime) / 1_000_000; // Convert to ms
            results.put(name, executionTimeMs);
            
            logger.info("Strategy {} completed in {} ms", name, executionTimeMs);
        }
        
        return results;
    }
    
    /**
     * Validates that the join columns exist in their respective tables.
     */
    private void validateJoinColumns(Table leftTable, Table rightTable, JoinCondition joinCondition) 
            throws ResultGenerationException {
        String leftColumn = joinCondition.leftColumn();
        String rightColumn = joinCondition.rightColumn();
        
        if (!leftTable.columnNames().contains(leftColumn)) {
            throw new ResultGenerationException(
                "Left join column '" + leftColumn + "' not found in table " + leftTable.name(),
                "join_executor",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        if (!rightTable.columnNames().contains(rightColumn)) {
            throw new ResultGenerationException(
                "Right join column '" + rightColumn + "' not found in table " + rightTable.name(),
                "join_executor",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        // Check that the columns are date columns or can be converted to dates
        if (!isDateCompatibleColumn(leftTable.column(leftColumn)) ||
            !isDateCompatibleColumn(rightTable.column(rightColumn))) {
            throw new ResultGenerationException(
                "Join columns must be date compatible for temporal joins",
                "join_executor",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Checks if a column is date-compatible (DateColumn or StringColumn with date format).
     */
    private boolean isDateCompatibleColumn(Column<?> column) {
        return column instanceof DateColumn;
        // In a real implementation, we could also check for StringColumn with date format
        // or other column types that could be converted to dates
    }
} 