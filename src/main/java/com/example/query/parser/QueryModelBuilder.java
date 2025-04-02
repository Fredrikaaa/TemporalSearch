package com.example.query.parser;

import com.example.query.model.*;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Contains;
import com.example.query.model.condition.Dependency;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Logical.LogicalOperator;
import com.example.query.model.condition.Ner;
import com.example.query.model.condition.Not;
import com.example.query.model.condition.Pos;
import com.example.query.model.condition.Temporal;
import com.example.query.binding.VariableRegistry;
import com.example.query.binding.VariableType;

import org.antlr.v4.runtime.tree.ParseTree;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Visitor implementation that builds a Query model from the parse tree.
 * Handles conversion from parse tree nodes to model objects.
 */
public class QueryModelBuilder extends QueryLangBaseVisitor<Object> {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    
    // Variable registry for tracking variables
    private final VariableRegistry variableRegistry = new VariableRegistry();
    
    /**
     * Creates a new QueryModelBuilder.
     */
    public QueryModelBuilder() {
        // No parameters needed
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
        List<SubquerySpec> subqueries = new ArrayList<>();
        Optional<JoinCondition> joinCondition = Optional.empty();

        // Extract select columns
        if (ctx.selectList() != null) {
            selectColumns = visitSelectList(ctx.selectList());
        }

        // Process join clauses if present
        if (ctx.joinClause() != null && !ctx.joinClause().isEmpty()) {
            for (QueryLangParser.JoinClauseContext joinCtx : ctx.joinClause()) {
                Object[] joinResult = (Object[]) visit(joinCtx);
                SubquerySpec subquery = (SubquerySpec) joinResult[0];
                JoinCondition jc = (JoinCondition) joinResult[1];
                
                subqueries.add(subquery);
                // Use the last join condition
                joinCondition = Optional.of(jc);
            }
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
            } else {
                granularity = Query.Granularity.SENTENCE;
                if (ctx.granularityClause().NUMBER() != null) {
                    granularitySize = Optional.of(Integer.parseInt(ctx.granularityClause().NUMBER().getText()));
                }
            }
        }

        // Validate variable registry - this automatically happens during variable registration now
        Set<String> validationErrors = variableRegistry.validate();
        if (!validationErrors.isEmpty()) {
            throw new IllegalStateException("Variable binding errors: " + String.join(", ", validationErrors));
        }

        return new Query(source, conditions, orderColumns, limit, granularity, granularitySize, selectColumns, variableRegistry, subqueries, joinCondition);
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
        return ctx.IDENTIFIER().getText();
    }

    @Override
    public Object visitContainsExpression(QueryLangParser.ContainsExpressionContext ctx) {
        List<String> terms = new ArrayList<>();
        for (var term : ctx.terms) {
            terms.add(unquote(term.getText()));
        }
        
        String variableName = null;
        boolean isVariable = false;
        String value = terms.get(0);
        
        if (ctx.var != null) {
            variableName = (String) visitVariable(ctx.var);
            isVariable = true;
            // Register variable in registry with TEXT_SPAN type
            variableRegistry.registerProducer(variableName, VariableType.TEXT_SPAN, "CONTAINS");
        }
        
        return new Contains(terms, variableName, isVariable, value);
    }

    @Override
    public Object visitNerExpression(QueryLangParser.NerExpressionContext ctx) {
        String type = (String) visitEntityType(ctx.type);
        String variableName = null;
        boolean isVariable = false;
        
        if (ctx.var != null) {
            variableName = (String) visitVariable(ctx.var);
            isVariable = true;
            // Register variable in registry with appropriate type based on NER entity type
            VariableType varType = determineNerVariableType(type);
            variableRegistry.registerProducer(variableName, varType, "NER");
        }
        
        String termValue = null;
        if (ctx.termValue != null) {
            termValue = (String) visitTerm(ctx.termValue);
        }
        
        return new Ner(type, termValue, variableName, isVariable);
    }

    // Helper method to determine variable type from NER entity type
    private VariableType determineNerVariableType(String nerType) {
        if (nerType == null) {
            return VariableType.ANY;
        }
        
        return switch (nerType.toUpperCase()) {
            case "PERSON", "ORGANIZATION", "LOCATION" -> VariableType.ENTITY;
            case "DATE", "TIME" -> VariableType.TEMPORAL;
            default -> VariableType.ENTITY;
        };
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
    public Object visitVariable(QueryLangParser.VariableContext ctx) {
        return "?" + ctx.IDENTIFIER().getText();
    }

    @Override
    public Object visitDateComparisonExpression(QueryLangParser.DateComparisonExpressionContext ctx) {
        String operator = ctx.comparisonOp().getText();
        int year = Integer.parseInt(ctx.year.getText());
        
        Temporal.ComparisonType compType = mapComparisonOp(operator);
        
        String variableName = null;
        if (ctx.var != null) {
            variableName = (String) visitVariable(ctx.var);
            // Register variable in registry with TEMPORAL type
            variableRegistry.registerProducer(variableName, VariableType.TEMPORAL, "TEMPORAL");
        }
        
        // Use the appropriate constructor based on whether we have a variable
        if (variableName != null) {
            return new Temporal(compType, year, variableName);
        }
        return new Temporal(compType, year);
    }
    
    @Override
    public Object visitDateOperatorExpression(QueryLangParser.DateOperatorExpressionContext ctx) {
        String operator = ctx.dateOperator().getText();
        Temporal.Type type = mapDateOperator(operator);
        
        Object dateValue = visitChildren(ctx.dateValue());
        LocalDateTime startDate;
        Optional<LocalDateTime> endDate = Optional.empty();
        
        if (dateValue instanceof Integer year) {
            startDate = LocalDateTime.of(year, 1, 1, 0, 0);
        } else if (dateValue instanceof int[] dateRange) {
            // Handle date range as array of ints [start, end]
            startDate = LocalDateTime.of(dateRange[0], 1, 1, 0, 0);
            endDate = Optional.of(LocalDateTime.of(dateRange[1], 12, 31, 23, 59, 59));
        } else {
            // Assume it's a single date
            startDate = (LocalDateTime) dateValue;
        }
        
        Optional<TemporalRange> range = Optional.empty();
        if (ctx.radius != null && ctx.unit != null) {
            int radius = Integer.parseInt(ctx.radius.getText());
            String unit = ctx.unit.getText();
            range = Optional.of(new TemporalRange(radius + unit));
        }
        
        String variableName = null;
        if (ctx.var != null) {
            variableName = (String) visitVariable(ctx.var);
            // Register variable in registry with TEMPORAL type
            variableRegistry.registerProducer(variableName, VariableType.TEMPORAL, "TEMPORAL");
        }
        
        // Create the temporal condition with the correct constructor
        return new Temporal(startDate, endDate, variableName != null ? Optional.of(variableName) : Optional.empty(), range, type);
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
        String governor = (String) visitGovernor(ctx.gov);
        String relation = (String) visitRelation(ctx.rel);
        String dependent = (String) visitDependent(ctx.dep);
        
        String variableName = null;
        boolean isVariable = false;
        
        if (ctx.var != null) {
            variableName = (String) visitVariable(ctx.var);
            isVariable = true;
            // Register variable in registry with DEPENDENCY type
            variableRegistry.registerProducer(variableName, VariableType.DEPENDENCY, "DEPENDENCY");
        }
        
        // Register consumed variables directly
        if (governor != null && governor.startsWith("?")) {
            variableRegistry.registerConsumer(governor, VariableType.ANY, "DEPENDENCY");
        }
        
        if (dependent != null && dependent.startsWith("?")) {
            variableRegistry.registerConsumer(dependent, VariableType.ANY, "DEPENDENCY");
        }
        
        return new Dependency(governor, relation, dependent, variableName, isVariable);
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
        String posTag = (String) visitPosTag(ctx.tag);
        String termValue = null;
        
        if (ctx.term() != null) {
            termValue = (String) visitTerm(ctx.term());
        }
        
        String variableName = null;
        boolean isVariable = false;
        
        if (ctx.var != null) {
            variableName = (String) visitVariable(ctx.var);
            isVariable = true;
            // Register variable in registry with POS_TAG type
            variableRegistry.registerProducer(variableName, VariableType.POS_TAG, "POS");
        }
        
        return new Pos(posTag, termValue, variableName, isVariable);
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

    public Query buildSubquery(ParseTree tree) {
        // Create a new QueryModelBuilder for the subquery to isolate variable bindings
        QueryModelBuilder subqueryBuilder = new QueryModelBuilder();
        return (Query) subqueryBuilder.visit(tree);
    }

    @Override
    public Object visitSubquery(QueryLangParser.SubqueryContext ctx) {
        // Get the source from the first identifier in the list
        String source = ctx.identifier(0).getText();
        
        // Create a new QueryModelBuilder for the subquery to isolate the variable scope
        QueryModelBuilder subqueryBuilder = new QueryModelBuilder();
        
        List<SelectColumn> selectColumns = subqueryBuilder.visitSelectList(ctx.selectList());
        
        List<Condition> conditions = new ArrayList<>();
        if (ctx.whereClause() != null) {
            conditions.addAll(subqueryBuilder.visitConditionList(ctx.whereClause().conditionList()));
        }
        
        // Create a subquery with minimal settings since additional settings will be ignored
        Query subquery = new Query(
            source,
            conditions,
            List.of(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            selectColumns,
            subqueryBuilder.variableRegistry
        );
        
        // Get the alias (which is the second identifier, or identified by the alias label)
        String alias = ctx.alias.getText();
        
        // Return the SubquerySpec
        return new SubquerySpec(subquery, alias);
    }

    @Override
    public Object visitJoinClause(QueryLangParser.JoinClauseContext ctx) {
        // Get the subquery
        SubquerySpec subquery = (SubquerySpec) visit(ctx.subquery());
        
        // Get the join condition
        JoinCondition joinCondition = (JoinCondition) visit(ctx.joinCondition());
        
        // Get the join type - defaults to INNER if not specified
        JoinCondition.JoinType joinType = JoinCondition.JoinType.INNER;
        if (ctx.joinType() != null) {
            if (ctx.joinType().LEFT() != null) {
                joinType = JoinCondition.JoinType.LEFT;
            } else if (ctx.joinType().RIGHT() != null) {
                joinType = JoinCondition.JoinType.RIGHT;
            }
        }
        
        // Update the join condition with the correct join type
        joinCondition = new JoinCondition(
            joinCondition.leftColumn(),
            joinCondition.rightColumn(),
            joinType,
            joinCondition.temporalPredicate(),
            joinCondition.proximityWindow()
        );
        
        // Return both the subquery and the join condition as a pair
        return new Object[] { subquery, joinCondition };
    }

    @Override
    public Object visitJoinCondition(QueryLangParser.JoinConditionContext ctx) {
        // Get the left and right columns
        String leftColumn = (String) visit(ctx.leftColumn);
        String rightColumn = (String) visit(ctx.rightColumn);
        
        // Get the temporal operator
        TemporalPredicate temporalPredicate = mapTemporalOperator(ctx.temporalOp().getText());
        
        // Check if there's a window specification
        Optional<Integer> proximityWindow = Optional.empty();
        if (ctx.window != null) {
            proximityWindow = Optional.of(Integer.parseInt(ctx.window.getText()));
        }
        
        // Create and return the join condition
        return new JoinCondition(
            leftColumn,
            rightColumn,
            JoinCondition.JoinType.INNER, // default, will be updated in visitJoinClause
            temporalPredicate,
            proximityWindow
        );
    }

    @Override
    public Object visitTemporalOp(QueryLangParser.TemporalOpContext ctx) {
        return ctx.getText();
    }

    @Override
    public Object visitQualifiedIdentifier(QueryLangParser.QualifiedIdentifierContext ctx) {
        String tableName = ctx.identifier(0).getText();
        String columnName;
        
        // Check if the right part is an identifier or a variable
        if (ctx.getChild(2) instanceof QueryLangParser.IdentifierContext) {
            columnName = ((QueryLangParser.IdentifierContext) ctx.getChild(2)).getText();
        } else {
            // Must be a variable
            columnName = (String) visit(ctx.getChild(2));
        }
        
        return tableName + "." + columnName;
    }

    // Helper method to map temporal operators to TemporalPredicate enum
    private TemporalPredicate mapTemporalOperator(String operator) {
        return switch (operator) {
            case "CONTAINS" -> TemporalPredicate.CONTAINS;
            case "CONTAINED_BY" -> TemporalPredicate.CONTAINED_BY;
            case "INTERSECT" -> TemporalPredicate.INTERSECT;
            case "NEAR" -> TemporalPredicate.PROXIMITY;
            default -> throw new IllegalStateException("Invalid temporal operator: " + operator);
        };
    }

    @Override
    public Object visitJoinColumn(QueryLangParser.JoinColumnContext ctx) {
        if (ctx.qualifiedIdentifier() != null) {
            return visit(ctx.qualifiedIdentifier());
        } else if (ctx.variable() != null) {
            return visit(ctx.variable());
        }
        throw new IllegalStateException("Invalid join column type");
    }
} 