package com.example.query.parser;

import com.example.query.model.*;
import org.antlr.v4.runtime.tree.ParseTree;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Visitor implementation that converts an ANTLR parse tree into our query model objects.
 */
public class QueryModelBuilder extends QueryLangBaseVisitor<Object> {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    
    @Override
    public Query visitQuery(QueryLangParser.QueryContext ctx) {
        String source = visitIdentifier(ctx.source);
        List<Condition> conditions = new ArrayList<>();
        List<OrderSpec> orderSpecs = new ArrayList<>();
        Optional<Integer> limit = Optional.empty();

        if (ctx.whereClause() != null) {
            for (QueryLangParser.ConditionContext condCtx : ctx.whereClause().condition()) {
                if (condCtx.getChildCount() > 1 && condCtx.getChild(0).getText().equals("(")) {
                    // Handle nested conditions
                    for (int i = 1; i < condCtx.getChildCount() - 1; i++) {
                        if (condCtx.getChild(i) instanceof QueryLangParser.ConditionContext) {
                            conditions.add((Condition) visit(condCtx.getChild(i)));
                        }
                    }
                } else {
                    conditions.add((Condition) visit(condCtx));
                }
            }
        }

        if (ctx.orderByClause() != null) {
            for (QueryLangParser.OrderSpecContext specCtx : ctx.orderByClause().orderSpec()) {
                orderSpecs.add(visitOrderSpec(specCtx));
            }
        }

        if (ctx.limitClause() != null) {
            limit = Optional.of(Integer.parseInt(ctx.limitClause().count.getText()));
        }

        return new Query(source, conditions, orderSpecs, limit);
    }

    @Override
    public String visitIdentifier(QueryLangParser.IdentifierContext ctx) {
        if (ctx.STRING() != null) {
            // Remove the quotes from the string
            String text = ctx.STRING().getText();
            return text.substring(1, text.length() - 1);
        }
        return ctx.IDENTIFIER().getText();
    }

    @Override
    public ContainsCondition visitContainsExpression(QueryLangParser.ContainsExpressionContext ctx) {
        String value = ctx.value.getText();
        // Remove the quotes from the string
        value = value.substring(1, value.length() - 1);
        return new ContainsCondition(value);
    }

    @Override
    public NerCondition visitNerExpression(QueryLangParser.NerExpressionContext ctx) {
        String type = visitIdentifier(ctx.type);
        
        if (ctx.identTarget != null) {
            return new NerCondition(type, visitIdentifier(ctx.identTarget), false);
        } else {
            return new NerCondition(type, ctx.varTarget.name.getText(), true);
        }
    }

    @Override
    public TemporalCondition visitTemporalExpression(QueryLangParser.TemporalExpressionContext ctx) {
        QueryLangParser.TemporalSpecContext spec = ctx.dateSpec;
        
        if (spec.BEFORE() != null) {
            LocalDateTime date = parseDateTime(spec.date.getText());
            return new TemporalCondition(TemporalCondition.Type.BEFORE, date);
        } else if (spec.AFTER() != null) {
            LocalDateTime date = parseDateTime(spec.date.getText());
            return new TemporalCondition(TemporalCondition.Type.AFTER, date);
        } else {
            LocalDateTime start = parseDateTime(spec.start.getText());
            LocalDateTime end = parseDateTime(spec.end.getText());
            return new TemporalCondition(start, end);
        }
    }

    @Override
    public DependencyCondition visitDependencyExpression(QueryLangParser.DependencyExpressionContext ctx) {
        String governor = visitIdentifier(ctx.governor);
        String relation = visitIdentifier(ctx.relation);
        String dependent = visitIdentifier(ctx.dependent);
        return new DependencyCondition(governor, relation, dependent);
    }

    @Override
    public OrderSpec visitOrderSpec(QueryLangParser.OrderSpecContext ctx) {
        String field = visitIdentifier(ctx.field);
        OrderSpec.Direction direction = OrderSpec.Direction.ASC; // Default to ascending
        
        if (ctx.DESC() != null) {
            direction = OrderSpec.Direction.DESC;
        }
        
        return new OrderSpec(field, direction);
    }

    private LocalDateTime parseDateTime(String text) {
        // Remove quotes from the string
        text = text.substring(1, text.length() - 1);
        return LocalDateTime.parse(text, DATE_FORMATTER);
    }

    /**
     * Convenience method to parse a query string and return a Query object.
     *
     * @param tree The ANTLR parse tree
     * @return The constructed Query object
     */
    public Query buildQuery(ParseTree tree) {
        return (Query) visit(tree);
    }
} 