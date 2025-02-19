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
	 * Visit a parse tree produced by {@link QueryLangParser#conditionList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConditionList(QueryLangParser.ConditionListContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#singleCondition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleCondition(QueryLangParser.SingleConditionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#nerExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNerExpression(QueryLangParser.NerExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#entityType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEntityType(QueryLangParser.EntityTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#entityTarget}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEntityTarget(QueryLangParser.EntityTargetContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#containsExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitContainsExpression(QueryLangParser.ContainsExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#dateExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDateExpression(QueryLangParser.DateExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#dateOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDateOperator(QueryLangParser.DateOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#dateValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDateValue(QueryLangParser.DateValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#rangeSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRangeSpec(QueryLangParser.RangeSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#dependsExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDependsExpression(QueryLangParser.DependsExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#governor}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGovernor(QueryLangParser.GovernorContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#dependent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDependent(QueryLangParser.DependentContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#relation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelation(QueryLangParser.RelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link QueryLangParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariable(QueryLangParser.VariableContext ctx);
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