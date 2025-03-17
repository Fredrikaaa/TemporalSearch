package com.example.query.parser;

import com.example.query.model.*;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Contains;
import com.example.query.model.condition.Dependency;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Ner;
import com.example.query.model.condition.Not;
import com.example.query.model.condition.Pos;
import com.example.query.model.condition.Temporal;
import com.example.query.model.TitleColumn;
import com.example.query.model.TimestampColumn;

import org.antlr.v4.runtime.tree.ParseTree;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Visitor implementation that builds a Query model from the parse tree.
 * Handles conversion from parse tree nodes to model objects.
 */
public class QueryModelBuilder extends QueryLangBaseVisitor<Object> {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    
    /**
     * Creates a new QueryModelBuilder.
     */
    public QueryModelBuilder() {
        // No parameters needed with SqliteAccessor singleton
    }
    
    @Override
    public Query visitQuery(QueryLangParser.QueryContext ctx) {
        String source = null;
        if (ctx.identifier() != null) {
            source = (String) visitIdentifier(ctx.identifier());
        }
        
        List<Condition> conditions = new ArrayList<>();
        List<String> orderColumns = new ArrayList<>();
        Optional<Integer> limit = Optional.empty();
        Query.Granularity granularity = Query.Granularity.DOCUMENT;
        Optional<Integer> granularitySize = Optional.empty();
        List<SelectColumn> selectColumns = new ArrayList<>();

        // Extract select columns
        if (ctx.selectList() != null) {
            selectColumns = visitSelectList(ctx.selectList());
        }

        if (ctx.whereClause() != null) {
            conditions.addAll(visitConditionList(ctx.whereClause().conditionList()));
        }

        if (ctx.orderByClause() != null) {
            for (QueryLangParser.OrderSpecContext specCtx : ctx.orderByClause().orderSpec()) {
                orderColumns.add((String) visitOrderSpec(specCtx));
            }
        }

        if (ctx.limitClause() != null) {
            limit = Optional.of(Integer.parseInt(ctx.limitClause().count.getText()));
        }
        
        if (ctx.granularityClause() != null) {
            if (ctx.granularityClause().DOCUMENT() != null) {
                granularity = Query.Granularity.DOCUMENT;
            } else if (ctx.granularityClause().SENTENCE() != null) {
                granularity = Query.Granularity.SENTENCE;
                if (ctx.granularityClause().NUMBER() != null) {
                    granularitySize = Optional.of(Integer.parseInt(ctx.granularityClause().NUMBER().getText()));
                }
            }
        }
        
        return new Query(source, conditions, orderColumns, limit, granularity, granularitySize, selectColumns);
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
        String variable = (String) visit(ctx.variable());
        return new VariableColumn(variable);
    }
    
    @Override
    public Object visitSnippetColumn(QueryLangParser.SnippetColumnContext ctx) {
        SnippetNode snippet = (SnippetNode) visit(ctx.snippetExpression());
        return new SnippetColumn(snippet);
    }
    
    @Override
    public Object visitTitleColumn(QueryLangParser.TitleColumnContext ctx) {
        return new TitleColumn();
    }
    
    @Override
    public Object visitTimestampColumn(QueryLangParser.TimestampColumnContext ctx) {
        return new TimestampColumn();
    }
    
    @Override
    public Object visitCountColumn(QueryLangParser.CountColumnContext ctx) {
        CountNode countNode = (CountNode) visit(ctx.countExpression());
        return new CountColumn(countNode);
    }
    
    @Override
    public Object visitIdentifierColumn(QueryLangParser.IdentifierColumnContext ctx) {
        String identifier = (String) visit(ctx.identifier());
        return new VariableColumn(identifier);
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
        if (ctx.condition().size() == 1) {
            // If there's only one condition, return it without creating a logical condition
            Object result = visit(ctx.condition(0));
            if (result instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<Condition> conditions = (List<Condition>) result;
                return conditions;
            } else if (result instanceof Condition) {
                return List.of((Condition) result);
            }
        }
        
        // Start with the first condition
        Object firstResult = visit(ctx.condition(0));
        Condition currentCondition;
        
        if (firstResult instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Condition> firstConditions = (List<Condition>) firstResult;
            if (firstConditions.size() == 1) {
                currentCondition = firstConditions.get(0);
            } else {
                throw new IllegalStateException("Unexpected multiple conditions in first result");
            }
        } else {
            currentCondition = (Condition) firstResult;
        }
        
        // Process the logical operations
        for (int i = 0; i < ctx.logicalOp().size(); i++) {
            // Get the logical operator
            String opText = ctx.logicalOp(i).getText();
            Logical.LogicalOperator operator;
            if (opText.equalsIgnoreCase("AND")) {
                operator = Logical.LogicalOperator.AND;
            } else if (opText.equalsIgnoreCase("OR")) {
                operator = Logical.LogicalOperator.OR;
            } else {
                throw new IllegalStateException("Unexpected logical operator: " + opText);
            }
            
            // Get the right operand
            Object rightResult = visit(ctx.condition(i + 1));
            Condition rightCondition;
            
            if (rightResult instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<Condition> rightConditions = (List<Condition>) rightResult;
                if (rightConditions.size() == 1) {
                    rightCondition = rightConditions.get(0);
                } else {
                    throw new IllegalStateException("Unexpected multiple conditions in right result");
                }
            } else {
                rightCondition = (Condition) rightResult;
            }
            
            // Create a logical condition
            currentCondition = new Logical(operator, currentCondition, rightCondition);
        }
        
        return List.of(currentCondition);
    }
    
    @Override
    public Object visitCondition(QueryLangParser.ConditionContext ctx) {
        return visit(ctx.getChild(0));
    }
    
    @Override
    public Object visitNotCondition(QueryLangParser.NotConditionContext ctx) {
        Object result = visit(ctx.atomicCondition());
        if (result instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Condition> conditions = (List<Condition>) result;
            if (conditions.size() == 1) {
                return new Not(conditions.get(0));
            }
            throw new IllegalStateException("Unexpected multiple conditions in NOT result");
        }
        return new Not((Condition) result);
    }
    
    @Override
    public Object visitAtomicCondition(QueryLangParser.AtomicConditionContext ctx) {
        if (ctx.singleCondition() != null) {
            return visit(ctx.singleCondition());
        } else if (ctx.LPAREN() != null) {
            return visit(ctx.conditionList());
        }
        throw new IllegalStateException("Unexpected atomic condition structure");
    }

    @Override
    public Object visitSingleCondition(QueryLangParser.SingleConditionContext ctx) {
        // The nesting logic is now handled in visitAtomicCondition
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
    public Object visitContainsWithVariableExpression(QueryLangParser.ContainsWithVariableExpressionContext ctx) {
        String variableName = (String) visit(ctx.variable());
        String term = unquote(ctx.terms.get(0).getText());
        
        // Remove the ? prefix from variable name if present
        if (variableName.startsWith("?")) {
            variableName = variableName.substring(1);
        }
        
        return new Contains(variableName, term);
    }

    @Override
    public Object visitContainsWithoutVariableExpression(QueryLangParser.ContainsWithoutVariableExpressionContext ctx) {
        List<String> terms = new ArrayList<>();
        for (var termCtx : ctx.terms) {
            terms.add(unquote(termCtx.getText()));
        }
        return new Contains(terms);
    }

    @Override
    public Object visitNerExpression(QueryLangParser.NerExpressionContext ctx) {
        String type = (String) visit(ctx.entityType());
        
        if (type == null) {
            throw new IllegalStateException("NER type cannot be null");
        }

        // Handle optional target
        if (ctx.entityTarget() == null) {
            return Ner.of(type);
        }

        String target = (String) visit(ctx.entityTarget());
        
        // If target is a variable, it's a variable binding condition
        if (ctx.entityTarget().variable() != null) {
            return Ner.withVariable(type, target);
        } else {
            return Ner.of(type);
        }
    }

    @Override
    public Object visitEntityType(QueryLangParser.EntityTypeContext ctx) {
        if (ctx.WILDCARD() != null) {
            return "*";
        }
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        }
        if (ctx.IDENTIFIER() != null) {
            return ctx.IDENTIFIER().getText();
        }
        // Handle NER type tokens
        if (ctx.PERSON() != null) return "PERSON";
        if (ctx.LOCATION() != null) return "LOCATION";
        if (ctx.ORGANIZATION() != null) return "ORGANIZATION";
        if (ctx.TIME() != null) return "TIME";
        if (ctx.MONEY() != null) return "MONEY";
        if (ctx.PERCENT() != null) return "PERCENT";
        
        throw new IllegalStateException("Invalid entity type");
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
        return "?" + ctx.IDENTIFIER().getText();
    }

    @Override
    public Object visitDateComparisonExpression(QueryLangParser.DateComparisonExpressionContext ctx) {
        String variable = (String) visit(ctx.variable());
        int year = Integer.parseInt(ctx.year.getText());
        Temporal.ComparisonType compType = mapComparisonOp(ctx.comparisonOp().getText());
        
        return new Temporal(compType, variable, year);
    }
    
    @Override
    public Object visitDateOperatorExpression(QueryLangParser.DateOperatorExpressionContext ctx) {
        String variable = (String) visit(ctx.variable());
        Temporal.Type type = mapDateOperator(ctx.dateOperator().getText());
        
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
            return new Temporal(type, variable, startDate, radiusValue);
        }
        
        if (endDate.isPresent()) {
            // TODO: Properly handle date ranges
            return new Temporal(type, variable, startDate);
        } else {
            return new Temporal(type, variable, startDate);
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

    private Temporal.ComparisonType mapComparisonOp(String operator) {
        return switch (operator) {
            case "<" -> Temporal.ComparisonType.LT;
            case ">" -> Temporal.ComparisonType.GT;
            case "<=" -> Temporal.ComparisonType.LE;
            case ">=" -> Temporal.ComparisonType.GE;
            case "==" -> Temporal.ComparisonType.EQ;
            default -> throw new IllegalStateException("Invalid comparison operator: " + operator);
        };
    }
    
    private Temporal.Type mapDateOperator(String operator) {
        return switch (operator) {
            case "CONTAINS" -> Temporal.Type.CONTAINS;
            case "CONTAINED_BY" -> Temporal.Type.CONTAINED_BY;
            case "INTERSECT" -> Temporal.Type.INTERSECT;
            case "NEAR" -> Temporal.Type.NEAR;
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

        // Check if any of the components is a variable
        boolean hasVariable = ctx.governor().variable() != null || 
                            ctx.dependent().variable() != null;

        if (hasVariable) {
            String variableName;
            if (ctx.governor().variable() != null) {
                variableName = governor.substring(1); // Remove the ? prefix
            } else {
                variableName = dependent.substring(1); // Remove the ? prefix
            }
            return new Dependency(variableName, governor, relation, dependent);
        } else {
            return new Dependency(governor, relation, dependent);
        }
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
        
        // For descending order, prefix with minus sign
        if (ctx.DESC() != null) {
            return "-" + field;
        }
        
        // For ascending order, just return the field name
        return field;
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

    @Override
    public Object visitPosExpression(QueryLangParser.PosExpressionContext ctx) {
        String posTag = (String) visit(ctx.posTag());
        String term = (String) visit(ctx.term());
        
        if (posTag == null || term == null) {
            throw new IllegalStateException("POS tag and term cannot be null");
        }

        // Check if term is a variable
        if (ctx.term().variable() != null) {
            String variableName = term.substring(1); // Remove the ? prefix
            return new Pos(variableName, posTag, term);
        } else {
            return new Pos(posTag, term);
        }
    }

    @Override
    public Object visitPosTag(QueryLangParser.PosTagContext ctx) {
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        }
        return visitIdentifier(ctx.identifier());
    }

    @Override
    public Object visitTerm(QueryLangParser.TermContext ctx) {
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        } else if (ctx.variable() != null) {
            return visit(ctx.variable());
        }
        return visitIdentifier(ctx.identifier());
    }
} 