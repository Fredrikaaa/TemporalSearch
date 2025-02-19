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
        String source = (String) visitIdentifier(ctx.source);
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
                orderSpecs.add((OrderSpec) visitOrderSpec(specCtx));
            }
        }

        if (ctx.limitClause() != null) {
            limit = Optional.of(Integer.parseInt(ctx.limitClause().count.getText()));
        }

        return new Query(source, conditions, orderSpecs, limit);
    }

    @Override
    public Object visitIdentifier(QueryLangParser.IdentifierContext ctx) {
        if (ctx.STRING() != null) {
            // Remove the quotes from the string
            String text = ctx.STRING().getText();
            return text.substring(1, text.length() - 1);
        }
        return ctx.IDENTIFIER().getText();
    }

    @Override
    public Object visitContainsExpression(QueryLangParser.ContainsExpressionContext ctx) {
        String value = ctx.value.getText();
        // Remove the quotes from the string
        value = value.substring(1, value.length() - 1);
        return new ContainsCondition(value);
    }

    @Override
    public Object visitNerExpression(QueryLangParser.NerExpressionContext ctx) {
        String type = (String) visitNerType(ctx.type);
        String target;
        boolean isVariable = false;

        if (ctx.target.variable() != null) {
            target = (String) visitVariable(ctx.target.variable());
            isVariable = true;
        } else {
            target = (String) visitIdentifier(ctx.target.identifier());
        }

        return new NerCondition(type, target, isVariable);
    }

    @Override
    public Object visitNerType(QueryLangParser.NerTypeContext ctx) {
        if (ctx.WILDCARD() != null) {
            return "*";
        }
        return visitIdentifier(ctx.identifier());
    }

    @Override
    public Object visitVariable(QueryLangParser.VariableContext ctx) {
        StringBuilder var = new StringBuilder(ctx.name.getText());
        if (ctx.variableModifier() != null) {
            if (ctx.variableModifier().WILDCARD() != null) {
                var.append("*");
            } else if (ctx.variableModifier().OPTIONAL() != null) {
                var.append("?");
            }
        }
        return var.toString();
    }

    @Override
    public Object visitTemporalExpression(QueryLangParser.TemporalExpressionContext ctx) {
        if (ctx.dateSpec != null) {
            return visitTemporalSpec(ctx.dateSpec);
        } else if (ctx.dateVar != null) {
            if (ctx.temporalOperator() != null) {
                // Handle DATE(?var) < "2020" or similar
                String dateVar = (String) visitVariable(ctx.dateVar);
                LocalDateTime compareDate = null;
                
                if (ctx.dateValue().STRING() != null) {
                    compareDate = parseDateTime(ctx.dateValue().STRING().getText());
                } else if (ctx.dateValue().subQuery() != null) {
                    // TODO: Handle subquery date comparison
                    throw new UnsupportedOperationException("Subquery date comparison not yet implemented");
                }

                TemporalCondition.Type type = getTemporalType(ctx.temporalOperator());
                return new TemporalCondition(type, dateVar, compareDate);
            } else {
                // Simple DATE(?var)
                return new TemporalCondition((String) visitVariable(ctx.dateVar));
            }
        }
        throw new IllegalStateException("Invalid temporal expression");
    }

    @Override
    public Object visitTemporalSpec(QueryLangParser.TemporalSpecContext spec) {
        if (spec.BEFORE() != null) {
            LocalDateTime date = getTemporalValue(spec.date);
            return new TemporalCondition(TemporalCondition.Type.BEFORE, date);
        } else if (spec.AFTER() != null) {
            LocalDateTime date = getTemporalValue(spec.date);
            return new TemporalCondition(TemporalCondition.Type.AFTER, date);
        } else if (spec.BETWEEN() != null) {
            LocalDateTime start = getTemporalValue(spec.start);
            LocalDateTime end = getTemporalValue(spec.end);
            return new TemporalCondition(start, end);
        } else if (spec.NEAR() != null) {
            LocalDateTime date = getTemporalValue(spec.date);
            String range = spec.range.getText();
            range = range.substring(1, range.length() - 1); // Remove quotes
            return new TemporalCondition(TemporalCondition.Type.NEAR, date, range);
        }
        throw new IllegalStateException("Invalid temporal specification");
    }

    private LocalDateTime getTemporalValue(QueryLangParser.TemporalValueContext ctx) {
        if (ctx.STRING() != null) {
            return parseDateTime(ctx.STRING().getText());
        } else if (ctx.variable() != null) {
            // TODO: Handle variable date resolution
            throw new UnsupportedOperationException("Variable date resolution not yet implemented");
        }
        throw new IllegalStateException("Invalid temporal value");
    }

    private TemporalCondition.Type getTemporalType(QueryLangParser.TemporalOperatorContext ctx) {
        if (ctx.BEFORE() != null) return TemporalCondition.Type.BEFORE;
        if (ctx.AFTER() != null) return TemporalCondition.Type.AFTER;
        if (ctx.BETWEEN() != null) return TemporalCondition.Type.BETWEEN;
        if (ctx.NEAR() != null) return TemporalCondition.Type.NEAR;
        throw new IllegalStateException("Invalid temporal operator");
    }

    @Override
    public Object visitDependencyExpression(QueryLangParser.DependencyExpressionContext ctx) {
        String governor = getDepComponent(ctx.governor);
        String relation = (String) visitIdentifier(ctx.relation);
        String dependent = getDepComponent(ctx.dependent);
        return new DependencyCondition(governor, relation, dependent);
    }

    private String getDepComponent(QueryLangParser.DepComponentContext ctx) {
        if (ctx.identifier() != null) {
            return (String) visitIdentifier(ctx.identifier());
        } else if (ctx.variable() != null) {
            return (String) visitVariable(ctx.variable());
        }
        throw new IllegalStateException("Invalid dependency component");
    }

    @Override
    public Object visitOrderSpec(QueryLangParser.OrderSpecContext ctx) {
        String field = (String) visitIdentifier(ctx.field);
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