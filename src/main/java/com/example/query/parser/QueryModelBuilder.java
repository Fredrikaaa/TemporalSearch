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
        String source = null;
        if (ctx.identifier() != null) {
            source = (String) visitIdentifier(ctx.identifier());
        }
        
        List<Condition> conditions = new ArrayList<>();
        List<OrderSpec> orderSpecs = new ArrayList<>();
        Optional<Integer> limit = Optional.empty();
        Optional<Query.Granularity> granularity = Optional.empty();
        Optional<Integer> granularitySize = Optional.empty();

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
        
        if (ctx.granularityClause() != null) {
            if (ctx.granularityClause().DOCUMENT() != null) {
                granularity = Optional.of(Query.Granularity.DOCUMENT);
            } else if (ctx.granularityClause().SENTENCE() != null) {
                granularity = Optional.of(Query.Granularity.SENTENCE);
                if (ctx.granularityClause().NUMBER() != null) {
                    granularitySize = Optional.of(Integer.parseInt(ctx.granularityClause().NUMBER().getText()));
                }
            }
        }

        return new Query(source, conditions, orderSpecs, limit, granularity, granularitySize);
    }

    @Override
    public List<SelectColumn> visitSelectList(QueryLangParser.SelectListContext ctx) {
        List<SelectColumn> columns = new ArrayList<>();
        for (QueryLangParser.SelectColumnContext colCtx : ctx.selectColumn()) {
            columns.add((SelectColumn) visit(colCtx));
        }
        return columns;
    }
    
    @Override
    public Object visitVariableColumn(QueryLangParser.VariableColumnContext ctx) {
        return visit(ctx.variable());
    }
    
    @Override
    public Object visitSnippetColumn(QueryLangParser.SnippetColumnContext ctx) {
        return visit(ctx.snippetExpression());
    }
    
    @Override
    public Object visitMetadataColumn(QueryLangParser.MetadataColumnContext ctx) {
        return visit(ctx.metadataExpression());
    }
    
    @Override
    public Object visitCountColumn(QueryLangParser.CountColumnContext ctx) {
        return visit(ctx.countExpression());
    }
    
    @Override
    public Object visitIdentifierColumn(QueryLangParser.IdentifierColumnContext ctx) {
        return visit(ctx.identifier());
    }
    
    @Override
    public Object visitSnippetExpression(QueryLangParser.SnippetExpressionContext ctx) {
        String variable = (String) visit(ctx.variable());
        int windowSize = SnippetNode.DEFAULT_WINDOW_SIZE;
        
        if (ctx.NUMBER() != null) {
            windowSize = Integer.parseInt(ctx.NUMBER().getText());
        }
        
        return new SnippetNode(variable, windowSize);
    }
    
    @Override
    public Object visitMetadataExpression(QueryLangParser.MetadataExpressionContext ctx) {
        return new MetadataNode();
    }
    
    @Override
    public Object visitCountAllExpression(QueryLangParser.CountAllExpressionContext ctx) {
        return CountNode.countAll();
    }
    
    @Override
    public Object visitCountUniqueExpression(QueryLangParser.CountUniqueExpressionContext ctx) {
        String variable = (String) visit(ctx.variable());
        return CountNode.countUnique(variable);
    }
    
    @Override
    public Object visitCountDocumentsExpression(QueryLangParser.CountDocumentsExpressionContext ctx) {
        return CountNode.countDocuments();
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
        List<String> terms = new ArrayList<>();
        for (var termCtx : ctx.terms) {
            terms.add(unquote(termCtx.getText()));
        }
        return new ContainsCondition(terms);
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
        return ctx.IDENTIFIER().getText();
    }

    @Override
    public Object visitDateComparisonExpression(QueryLangParser.DateComparisonExpressionContext ctx) {
        String variable = (String) visit(ctx.variable());
        int year = Integer.parseInt(ctx.year.getText());
        TemporalCondition.ComparisonType compType = mapComparisonOp(ctx.comparisonOp().getText());
        
        return new TemporalCondition(compType, variable, year);
    }
    
    @Override
    public Object visitDateOperatorExpression(QueryLangParser.DateOperatorExpressionContext ctx) {
        String variable = (String) visit(ctx.variable());
        TemporalCondition.Type type = mapDateOperator(ctx.dateOperator().getText());
        
        // Process the date value
        Object dateValueResult = visit(ctx.dateValue());
        LocalDateTime startDate = null;
        Optional<LocalDateTime> endDate = Optional.empty();
        
        if (dateValueResult instanceof LocalDateTime) {
            startDate = (LocalDateTime) dateValueResult;
        } else if (dateValueResult instanceof LocalDateTime[] dateRange) {
            startDate = dateRange[0];
            endDate = Optional.of(dateRange[1]);
        }
        
        // Handle radius if present
        if (ctx.radius != null) {
            String radiusValue = ctx.radius.getText() + ctx.unit.getText();
            return new TemporalCondition(type, variable, startDate, radiusValue);
        }
        
        if (endDate.isPresent()) {
            // TODO: Properly handle date ranges
            return new TemporalCondition(type, variable, startDate);
        } else {
            return new TemporalCondition(type, variable, startDate);
        }
    }
    
    @Override
    public Object visitDateRange(QueryLangParser.DateRangeContext ctx) {
        int startYear = Integer.parseInt(ctx.start.getText());
        int endYear = Integer.parseInt(ctx.end.getText());
        LocalDateTime startDate = LocalDateTime.of(startYear, 1, 1, 0, 0);
        LocalDateTime endDate = LocalDateTime.of(endYear, 12, 31, 23, 59, 59);
        return new LocalDateTime[] { startDate, endDate };
    }
    
    @Override
    public Object visitSingleYear(QueryLangParser.SingleYearContext ctx) {
        int year = Integer.parseInt(ctx.single.getText());
        return LocalDateTime.of(year, 1, 1, 0, 0);
    }

    private TemporalCondition.ComparisonType mapComparisonOp(String operator) {
        return switch (operator) {
            case "<" -> TemporalCondition.ComparisonType.LT;
            case ">" -> TemporalCondition.ComparisonType.GT;
            case "<=" -> TemporalCondition.ComparisonType.LE;
            case ">=" -> TemporalCondition.ComparisonType.GE;
            case "==" -> TemporalCondition.ComparisonType.EQ;
            default -> throw new IllegalStateException("Invalid comparison operator: " + operator);
        };
    }
    
    private TemporalCondition.Type mapDateOperator(String operator) {
        return switch (operator) {
            case "CONTAINS" -> TemporalCondition.Type.CONTAINS;
            case "CONTAINED_BY" -> TemporalCondition.Type.CONTAINED_BY;
            case "INTERSECT" -> TemporalCondition.Type.INTERSECT;
            case "NEAR" -> TemporalCondition.Type.NEAR;
            default -> throw new IllegalStateException("Invalid date operator: " + operator);
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
        String field;
        
        // Extract the field name from either identifier or variable
        if (ctx.identifier() != null) {
            field = (String) visitIdentifier(ctx.identifier());
        } else if (ctx.variable() != null) {
            field = (String) visit(ctx.variable());
        } else {
            throw new IllegalStateException("OrderSpec must have either identifier or variable");
        }
        
        OrderSpec.Direction direction = OrderSpec.Direction.ASC; // Default to ascending
        
        if (ctx.DESC() != null) {
            direction = OrderSpec.Direction.DESC;
        }
        
        return new OrderSpec(field, direction);
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
    
    public Query buildQuery(ParseTree tree) {
        return (Query) visit(tree);
    }
} 