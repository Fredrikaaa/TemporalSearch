package com.example.query.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Represents a parsed query in our query language.
 * This is the root class that holds all components of a query.
 */
public class Query {
    private final String source;
    private final List<Condition> conditions;
    private final List<OrderSpec> orderBy;
    private final Optional<Integer> limit;

    public Query(String source) {
        this.source = source;
        this.conditions = new ArrayList<>();
        this.orderBy = new ArrayList<>();
        this.limit = Optional.empty();
    }

    public Query(String source, List<Condition> conditions, List<OrderSpec> orderBy, Optional<Integer> limit) {
        this.source = source;
        this.conditions = conditions;
        this.orderBy = orderBy;
        this.limit = limit;
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
        
        sb.append('}');
        return sb.toString();
    }
} 