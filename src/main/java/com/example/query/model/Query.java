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

    public Query(String source) {
        this.source = source;
        this.conditions = new ArrayList<>();
        this.orderBy = new ArrayList<>();
        this.limit = Optional.empty();
        this.granularity = Optional.empty();
        this.granularitySize = Optional.empty();
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

    public void addCondition(Condition condition) {
        conditions.add(condition);
    }

    public void addOrderSpec(OrderSpec spec) {
        orderBy.add(spec);
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