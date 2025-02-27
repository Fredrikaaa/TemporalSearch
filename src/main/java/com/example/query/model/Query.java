package com.example.query.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Represents a parsed query in our query language.
 * This is the root class that holds all components of a query.
 */
public class Query {
    /**
     * Enum for the different granularity levels.
     */
    public enum Granularity {
        DOCUMENT,
        SENTENCE
    }

    private final String source;
    private final List<Condition> conditions;
    private final List<OrderSpec> orderBy;
    private final Optional<Integer> limit;
    private final Optional<Granularity> granularity;
    private final Optional<Integer> granularitySize;
    private List<SelectColumn> selectColumns;

    public Query(String source) {
        this.source = source;
        this.conditions = new ArrayList<>();
        this.orderBy = new ArrayList<>();
        this.limit = Optional.empty();
        this.granularity = Optional.empty();
        this.granularitySize = Optional.empty();
        this.selectColumns = new ArrayList<>();
    }

    public Query(String source, List<Condition> conditions, List<OrderSpec> orderBy, 
                 Optional<Integer> limit, Optional<Granularity> granularity, 
                 Optional<Integer> granularitySize) {
        this.source = source;
        this.conditions = conditions;
        this.orderBy = orderBy;
        this.limit = limit;
        this.granularity = granularity;
        this.granularitySize = granularitySize;
        this.selectColumns = new ArrayList<>();
    }
    
    /**
     * Creates a new Query with all parameters including select columns.
     */
    public Query(String source, List<Condition> conditions, List<OrderSpec> orderBy, 
                 Optional<Integer> limit, Optional<Granularity> granularity, 
                 Optional<Integer> granularitySize, List<SelectColumn> selectColumns) {
        this.source = source;
        this.conditions = conditions;
        this.orderBy = orderBy;
        this.limit = limit;
        this.granularity = granularity;
        this.granularitySize = granularitySize;
        this.selectColumns = selectColumns != null ? selectColumns : new ArrayList<>();
    }

    public String getSource() {
        return source;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public List<OrderSpec> getOrderBy() {
        return orderBy;
    }

    public Optional<Integer> getLimit() {
        return limit;
    }
    
    public Optional<Granularity> getGranularity() {
        return granularity;
    }
    
    public Optional<Integer> getGranularitySize() {
        return granularitySize;
    }
    
    /**
     * Gets the list of select columns from the SELECT clause.
     * 
     * @return List of select columns
     */
    public List<SelectColumn> getSelectColumns() {
        return selectColumns;
    }
    
    /**
     * Sets the list of select columns from the SELECT clause.
     * 
     * @param selectColumns The list of select columns
     */
    public void setSelectColumns(List<SelectColumn> selectColumns) {
        this.selectColumns = selectColumns != null ? selectColumns : new ArrayList<>();
    }

    public void addCondition(Condition condition) {
        conditions.add(condition);
    }

    public void addOrderSpec(OrderSpec spec) {
        orderBy.add(spec);
    }
    
    /**
     * Adds a single select column to the query.
     * 
     * @param column The select column to add
     */
    public void addSelectColumn(SelectColumn column) {
        if (column != null) {
            this.selectColumns.add(column);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Query{source='").append(source).append('\'');
        
        if (!conditions.isEmpty()) {
            sb.append(", conditions=").append(conditions);
        }
        
        if (!orderBy.isEmpty()) {
            sb.append(", orderBy=").append(orderBy);
        }
        
        limit.ifPresent(l -> sb.append(", limit=").append(l));
        
        granularity.ifPresent(g -> {
            sb.append(", granularity=").append(g);
            granularitySize.ifPresent(s -> sb.append("(").append(s).append(")"));
        });
        
        sb.append('}');
        return sb.toString();
    }
} 