// Generated from com/example/query/parser/QueryLang.g4 by ANTLR 4.13.1
package com.example.query.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link QueryLangParser}.
 */
public interface QueryLangListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(QueryLangParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(QueryLangParser.QueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(QueryLangParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(QueryLangParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(QueryLangParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(QueryLangParser.WhereClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#conditionList}.
	 * @param ctx the parse tree
	 */
	void enterConditionList(QueryLangParser.ConditionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#conditionList}.
	 * @param ctx the parse tree
	 */
	void exitConditionList(QueryLangParser.ConditionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#singleCondition}.
	 * @param ctx the parse tree
	 */
	void enterSingleCondition(QueryLangParser.SingleConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#singleCondition}.
	 * @param ctx the parse tree
	 */
	void exitSingleCondition(QueryLangParser.SingleConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#nerExpression}.
	 * @param ctx the parse tree
	 */
	void enterNerExpression(QueryLangParser.NerExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#nerExpression}.
	 * @param ctx the parse tree
	 */
	void exitNerExpression(QueryLangParser.NerExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#entityType}.
	 * @param ctx the parse tree
	 */
	void enterEntityType(QueryLangParser.EntityTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#entityType}.
	 * @param ctx the parse tree
	 */
	void exitEntityType(QueryLangParser.EntityTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#entityTarget}.
	 * @param ctx the parse tree
	 */
	void enterEntityTarget(QueryLangParser.EntityTargetContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#entityTarget}.
	 * @param ctx the parse tree
	 */
	void exitEntityTarget(QueryLangParser.EntityTargetContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#containsExpression}.
	 * @param ctx the parse tree
	 */
	void enterContainsExpression(QueryLangParser.ContainsExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#containsExpression}.
	 * @param ctx the parse tree
	 */
	void exitContainsExpression(QueryLangParser.ContainsExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#dateExpression}.
	 * @param ctx the parse tree
	 */
	void enterDateExpression(QueryLangParser.DateExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#dateExpression}.
	 * @param ctx the parse tree
	 */
	void exitDateExpression(QueryLangParser.DateExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#dateOperator}.
	 * @param ctx the parse tree
	 */
	void enterDateOperator(QueryLangParser.DateOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#dateOperator}.
	 * @param ctx the parse tree
	 */
	void exitDateOperator(QueryLangParser.DateOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#dateValue}.
	 * @param ctx the parse tree
	 */
	void enterDateValue(QueryLangParser.DateValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#dateValue}.
	 * @param ctx the parse tree
	 */
	void exitDateValue(QueryLangParser.DateValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#rangeSpec}.
	 * @param ctx the parse tree
	 */
	void enterRangeSpec(QueryLangParser.RangeSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#rangeSpec}.
	 * @param ctx the parse tree
	 */
	void exitRangeSpec(QueryLangParser.RangeSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#dependsExpression}.
	 * @param ctx the parse tree
	 */
	void enterDependsExpression(QueryLangParser.DependsExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#dependsExpression}.
	 * @param ctx the parse tree
	 */
	void exitDependsExpression(QueryLangParser.DependsExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#governor}.
	 * @param ctx the parse tree
	 */
	void enterGovernor(QueryLangParser.GovernorContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#governor}.
	 * @param ctx the parse tree
	 */
	void exitGovernor(QueryLangParser.GovernorContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#dependent}.
	 * @param ctx the parse tree
	 */
	void enterDependent(QueryLangParser.DependentContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#dependent}.
	 * @param ctx the parse tree
	 */
	void exitDependent(QueryLangParser.DependentContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterRelation(QueryLangParser.RelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitRelation(QueryLangParser.RelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterVariable(QueryLangParser.VariableContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitVariable(QueryLangParser.VariableContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#subQuery}.
	 * @param ctx the parse tree
	 */
	void enterSubQuery(QueryLangParser.SubQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#subQuery}.
	 * @param ctx the parse tree
	 */
	void exitSubQuery(QueryLangParser.SubQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#orderByClause}.
	 * @param ctx the parse tree
	 */
	void enterOrderByClause(QueryLangParser.OrderByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#orderByClause}.
	 * @param ctx the parse tree
	 */
	void exitOrderByClause(QueryLangParser.OrderByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#orderSpec}.
	 * @param ctx the parse tree
	 */
	void enterOrderSpec(QueryLangParser.OrderSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#orderSpec}.
	 * @param ctx the parse tree
	 */
	void exitOrderSpec(QueryLangParser.OrderSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void enterLimitClause(QueryLangParser.LimitClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void exitLimitClause(QueryLangParser.LimitClauseContext ctx);
}