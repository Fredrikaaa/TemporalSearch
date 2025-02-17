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
	 * Enter a parse tree produced by {@link QueryLangParser#condition}.
	 * @param ctx the parse tree
	 */
	void enterCondition(QueryLangParser.ConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#condition}.
	 * @param ctx the parse tree
	 */
	void exitCondition(QueryLangParser.ConditionContext ctx);
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
	 * Enter a parse tree produced by {@link QueryLangParser#temporalExpression}.
	 * @param ctx the parse tree
	 */
	void enterTemporalExpression(QueryLangParser.TemporalExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#temporalExpression}.
	 * @param ctx the parse tree
	 */
	void exitTemporalExpression(QueryLangParser.TemporalExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#temporalSpec}.
	 * @param ctx the parse tree
	 */
	void enterTemporalSpec(QueryLangParser.TemporalSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#temporalSpec}.
	 * @param ctx the parse tree
	 */
	void exitTemporalSpec(QueryLangParser.TemporalSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link QueryLangParser#dependencyExpression}.
	 * @param ctx the parse tree
	 */
	void enterDependencyExpression(QueryLangParser.DependencyExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link QueryLangParser#dependencyExpression}.
	 * @param ctx the parse tree
	 */
	void exitDependencyExpression(QueryLangParser.DependencyExpressionContext ctx);
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