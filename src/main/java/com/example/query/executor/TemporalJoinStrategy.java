package com.example.query.executor;

import tech.tablesaw.api.Table;

import com.example.query.model.JoinCondition;
import com.example.query.result.ResultGenerationException;

/**
 * Strategy interface for temporal join operations.
 * Implementations provide different algorithms for executing temporal joins.
 */
public interface TemporalJoinStrategy {
    /**
     * Executes a temporal join between two tables.
     * 
     * @param leftTable The left table in the join
     * @param rightTable The right table in the join
     * @param joinCondition The join condition containing columns and predicates
     * @return The joined table
     * @throws ResultGenerationException if the join fails
     */
    Table execute(Table leftTable, Table rightTable, JoinCondition joinCondition) 
            throws ResultGenerationException;
} 