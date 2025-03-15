package com.example.query.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.example.query.model.condition.Condition;

/**
 * Represents a query in the query language.
 * A query consists of:
 * - A source (e.g. "wikipedia")
 * - A list of conditions
 * - Optional order by specifications (column names, prefix with "-" for descending)
 * - Optional limit
 * - Granularity settings
 * - Optional granularity size
 * - List of columns to select
 */
public record Query(
    String source,
    List<Condition> conditions,
    List<String> orderBy,
    Optional<Integer> limit,
    Granularity granularity,
    Optional<Integer> granularitySize,
    List<SelectColumn> selectColumns
) {
    public enum Granularity {
        DOCUMENT,
        SENTENCE
    }

    /**
     * Creates a query with validation.
     */
    public Query {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(conditions, "Conditions cannot be null");
        Objects.requireNonNull(orderBy, "Order by specifications cannot be null");
        Objects.requireNonNull(limit, "Limit cannot be null");
        Objects.requireNonNull(granularity, "Granularity cannot be null");
        Objects.requireNonNull(granularitySize, "Granularity size cannot be null");
        Objects.requireNonNull(selectColumns, "Select columns cannot be null");

        // Make defensive copies
        conditions = List.copyOf(conditions);
        orderBy = List.copyOf(orderBy);
        selectColumns = List.copyOf(selectColumns);
    }

    /**
     * Creates a query with just a source.
     */
    public Query(String source) {
        this(source, List.of(), List.of(), Optional.empty(), Granularity.DOCUMENT, Optional.empty(), List.of());
    }

    /**
     * Creates a query with source and conditions.
     */
    public Query(String source, List<Condition> conditions) {
        this(source, conditions, List.of(), Optional.empty(), Granularity.DOCUMENT, Optional.empty(), List.of());
    }

    /**
     * Creates a query with source, conditions, and granularity.
     */
    public Query(String source, List<Condition> conditions, Granularity granularity) {
        this(source, conditions, List.of(), Optional.empty(), granularity, Optional.empty(), List.of());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FROM ").append(source);
        
        if (!selectColumns.isEmpty()) {
            sb.append(" SELECT ");
            for (int i = 0; i < selectColumns.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(selectColumns.get(i));
            }
        }
        
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            for (int i = 0; i < conditions.size(); i++) {
                if (i > 0) sb.append(" AND ");
                sb.append(conditions.get(i));
            }
        }
        
        if (!orderBy.isEmpty()) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < orderBy.size(); i++) {
                if (i > 0) sb.append(", ");
                String column = orderBy.get(i);
                if (column.startsWith("-")) {
                    sb.append(column.substring(1)).append(" DESC");
                } else {
                    sb.append(column).append(" ASC");
                }
            }
        }
        
        limit.ifPresent(l -> sb.append(" LIMIT ").append(l));
        
        return sb.toString();
    }
} 