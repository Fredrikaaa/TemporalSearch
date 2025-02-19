// Generated from com/example/query/parser/QueryLang.g4 by ANTLR 4.13.1
package com.example.query.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link QueryLangParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface QueryLangVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(QueryLangParser.QueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(QueryLangParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#whereClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhereClause(QueryLangParser.WhereClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#condition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCondition(QueryLangParser.ConditionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#containsExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitContainsExpression(QueryLangParser.ContainsExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#nerExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNerExpression(QueryLangParser.NerExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#nerType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNerType(QueryLangParser.NerTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#nerTarget}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNerTarget(QueryLangParser.NerTargetContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#temporalExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTemporalExpression(QueryLangParser.TemporalExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#temporalSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTemporalSpec(QueryLangParser.TemporalSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#temporalValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTemporalValue(QueryLangParser.TemporalValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#temporalOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTemporalOperator(QueryLangParser.TemporalOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#dateValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDateValue(QueryLangParser.DateValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#dependencyExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDependencyExpression(QueryLangParser.DependencyExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#depComponent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDepComponent(QueryLangParser.DepComponentContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariable(QueryLangParser.VariableContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#variableModifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariableModifier(QueryLangParser.VariableModifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#subQuery}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubQuery(QueryLangParser.SubQueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#orderByClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrderByClause(QueryLangParser.OrderByClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#orderSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrderSpec(QueryLangParser.OrderSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#limitClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLimitClause(QueryLangParser.LimitClauseContext ctx);
}