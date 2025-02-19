package com.example.query.parser;

import com.example.query.model.*;
import org.antlr.v4.runtime.tree.ParseTree;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Visitor implementation that converts an ANTLR parse tree into our query model objects.
 */
public class QueryModelBuilder extends QueryLangBaseVisitor<Object> {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    
    @Override
    public Query visitQuery(QueryLangParser.QueryContext ctx) {
        String source = (String) visitIdentifier(ctx.source);
        List<Condition> conditions = new ArrayList<>();
        List<OrderSpec> orderSpecs = new ArrayList<>();
        Optional<Integer> limit = Optional.empty();

        if (ctx.whereClause() != null) {
            conditions.addAll(visitConditionList(ctx.whereClause().conditionList()));
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
    public List<Condition> visitConditionList(QueryLangParser.ConditionListContext ctx) {
        List<Condition> conditions = new ArrayList<>();
        for (QueryLangParser.SingleConditionContext condCtx : ctx.singleCondition()) {
            Object result = visit(condCtx);
            if (result instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<Condition> nestedConditions = (List<Condition>) result;
                conditions.addAll(nestedConditions);
            } else {
                conditions.add((Condition) result);
            }
        }
        return conditions;
    }

    @Override
    public Object visitSingleCondition(QueryLangParser.SingleConditionContext ctx) {
        if (ctx.getChildCount() > 1 && ctx.getChild(0).getText().equals("(")) {
            // Handle nested conditions
            QueryLangParser.ConditionListContext listCtx = ctx.conditionList();
            if (listCtx != null) {
                return visitConditionList(listCtx);
            }
        }
        return super.visitSingleCondition(ctx);
    }

    @Override
    public Object visitIdentifier(QueryLangParser.IdentifierContext ctx) {
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        }
        return ctx.IDENTIFIER().getText();
    }

    @Override
    public Object visitContainsExpression(QueryLangParser.ContainsExpressionContext ctx) {
        return new ContainsCondition(unquote(ctx.STRING().getText()));
    }

    @Override
    public Object visitNerExpression(QueryLangParser.NerExpressionContext ctx) {
        String type = (String) visit(ctx.entityType());
        String target = (String) visit(ctx.entityTarget());
        boolean isVariable = ctx.entityTarget().variable() != null;
        
        if (type == null || target == null) {
            throw new IllegalStateException("NER type and target cannot be null");
        }
        
        return new NerCondition(type, target, isVariable);
    }

    @Override
    public Object visitEntityType(QueryLangParser.EntityTypeContext ctx) {
        if (ctx.WILDCARD() != null) {
            return "*";
        }
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        }
        return visitIdentifier(ctx.identifier());
    }

    @Override
    public Object visitEntityTarget(QueryLangParser.EntityTargetContext ctx) {
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        }
        return visit(ctx.variable());
    }

    @Override
    public Object visitVariable(QueryLangParser.VariableContext ctx) {
        StringBuilder var = new StringBuilder();
        var.append(ctx.IDENTIFIER().getText());
        if (ctx.WILDCARD() != null) {
            var.append("*");
        }
        return var.toString();
    }

    @Override
    public Object visitDateExpression(QueryLangParser.DateExpressionContext ctx) {
        if (ctx.NEAR() != null) {
            // Handle NEAR with range
            String dateVar = (String) visitVariable(ctx.dateVar);
            LocalDateTime compareDate = null;
            
            if (ctx.dateCompareValue != null && ctx.dateCompareValue.STRING() != null) {
                compareDate = parseDateTime(ctx.dateCompareValue.STRING().getText());
            } else if (ctx.dateCompareValue != null && ctx.dateCompareValue.subQuery() != null) {
                throw new UnsupportedOperationException("Subquery date comparison not yet implemented");
            }

            String range = unquote(ctx.dateRange.STRING().getText());
            return new TemporalCondition(TemporalCondition.Type.NEAR, dateVar, compareDate, range);
        } else if (ctx.dateOp != null) {
            // Handle DATE(?var) < "2020" or similar
            String dateVar = (String) visitVariable(ctx.dateVar);
            LocalDateTime compareDate = null;
            
            if (ctx.dateCompareValue != null && ctx.dateCompareValue.STRING() != null) {
                compareDate = parseDateTime(ctx.dateCompareValue.STRING().getText());
            } else if (ctx.dateCompareValue != null && ctx.dateCompareValue.subQuery() != null) {
                throw new UnsupportedOperationException("Subquery date comparison not yet implemented");
            }

            TemporalCondition.Type type = getTemporalType(ctx.dateOp.getText());
            return new TemporalCondition(type, dateVar, compareDate);
        } else if (ctx.dateVar != null) {
            // Simple DATE(?var)
            return new TemporalCondition((String) visitVariable(ctx.dateVar));
        } else if (ctx.dateString != null) {
            // Simple DATE "2020"
            return new TemporalCondition(TemporalCondition.Type.NEAR, parseDateTime(ctx.dateString.getText()));
        }
        throw new IllegalStateException("Invalid date expression");
    }

    private TemporalCondition.Type getTemporalType(String operator) {
        return switch (operator) {
            case "<" -> TemporalCondition.Type.BEFORE;
            case ">" -> TemporalCondition.Type.AFTER;
            case "<=" -> TemporalCondition.Type.BEFORE;  // Using BEFORE for <=
            case ">=" -> TemporalCondition.Type.AFTER;   // Using AFTER for >=
            case "==" -> TemporalCondition.Type.NEAR;    // Using NEAR for exact matches
            default -> throw new IllegalStateException("Invalid temporal operator: " + operator);
        };
    }

    @Override
    public Object visitDependsExpression(QueryLangParser.DependsExpressionContext ctx) {
        String governor = (String) visit(ctx.governor());
        String relation = (String) visit(ctx.relation());
        String dependent = (String) visit(ctx.dependent());
        
        if (governor == null || relation == null || dependent == null) {
            throw new IllegalStateException("DEPENDS components cannot be null");
        }
        
        return new DependencyCondition(governor, relation, dependent);
    }

    @Override
    public Object visitGovernor(QueryLangParser.GovernorContext ctx) {
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        } else if (ctx.variable() != null) {
            return visit(ctx.variable());
        }
        return visitIdentifier(ctx.identifier());
    }

    @Override
    public Object visitDependent(QueryLangParser.DependentContext ctx) {
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        } else if (ctx.variable() != null) {
            return visit(ctx.variable());
        }
        return visitIdentifier(ctx.identifier());
    }

    @Override
    public Object visitRelation(QueryLangParser.RelationContext ctx) {
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        }
        return visitIdentifier(ctx.identifier());
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

    @Override
    public Object visitSubQuery(QueryLangParser.SubQueryContext ctx) {
        throw new UnsupportedOperationException("Subqueries are not yet supported");
    }

    private LocalDateTime parseDateTime(String text) {
        text = unquote(text);
        try {
            // Try parsing as date-time first
            return LocalDateTime.parse(text, DATE_TIME_FORMATTER);
        } catch (Exception e) {
            // If that fails, try parsing as date and convert to start of day
            return LocalDateTime.of(LocalDate.parse(text, DATE_FORMATTER), java.time.LocalTime.MIN);
        }
    }

    private String unquote(String text) {
        if (text.startsWith("\"") && text.endsWith("\"")) {
            return text.substring(1, text.length() - 1);
        }
        return text;
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