package com.example.query.parser;

import com.example.query.QueryParseException;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for validating the ANTLR grammar's ability to parse various query syntax patterns.
 * These tests focus on validating the syntax parsing capabilities, not the semantic validity
 * of the queries or the construction of AST nodes.
 */
@DisplayName("Query Syntax Validation Tests")
public class QuerySyntaxValidationTest {

    /**
     * Parses a query string using the ANTLR parser directly and returns the parse tree.
     * This method bypasses the semantic validation and AST creation to focus only on syntax.
     */
    private ParseTree parseQuery(String queryString) throws QueryParseException {
        CharStream input = CharStreams.fromString(queryString);
        QueryLangLexer lexer = new QueryLangLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new RuntimeException("Lexer error at " + line + ":" + charPositionInLine + " - " + msg);
            }
        });

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        QueryLangParser parser = new QueryLangParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new RuntimeException("Parser error at " + line + ":" + charPositionInLine + " - " + msg);
            }
        });

        return parser.query();
    }

    /**
     * Helper method to assert that a query string is syntactically valid.
     */
    private void assertValidSyntax(String queryString) {
        assertDoesNotThrow(() -> parseQuery(queryString),
                "Query should have valid syntax: " + queryString);
    }

    /**
     * Helper method to assert that a query string has syntax errors.
     */
    private void assertInvalidSyntax(String queryString) {
        assertThrows(RuntimeException.class, () -> parseQuery(queryString),
                "Query should have invalid syntax: " + queryString);
    }

    @Test
    @DisplayName("Basic SELECT queries with various column types")
    void testBasicSelectQueries() {
        // Variable column
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\")");
        
        // Multiple columns
        assertValidSyntax("SELECT ?person, ?organization FROM documents WHERE CONTAINS(\"test\")");
        
        // Identifier column (just a named field)
        assertValidSyntax("SELECT author FROM documents WHERE CONTAINS(\"test\")");
    }

    @Test
    @DisplayName("SELECT with snippet expressions")
    void testSnippetExpressions() {
        // Basic snippet
        assertValidSyntax("SELECT SNIPPET(?person) FROM documents WHERE CONTAINS(\"test\")");
        
        // Snippet with window
        assertValidSyntax("SELECT SNIPPET(?person, WINDOW=5) FROM documents WHERE CONTAINS(\"test\")");
        
        // Multiple snippet expressions
        assertValidSyntax("SELECT SNIPPET(?person), SNIPPET(?org, WINDOW=10) FROM documents WHERE CONTAINS(\"test\")");
    }

    @Test
    @DisplayName("SELECT with metadata expressions")
    void testMetadataExpressions() {
        // Just metadata
        assertValidSyntax("SELECT METADATA FROM documents WHERE CONTAINS(\"test\")");
        
        // Metadata with other columns
        assertValidSyntax("SELECT METADATA, ?person FROM documents WHERE CONTAINS(\"test\")");
    }

    @Test
    @DisplayName("SELECT with count expressions")
    void testCountExpressions() {
        // COUNT(*)
        assertValidSyntax("SELECT COUNT(*) FROM documents WHERE CONTAINS(\"test\")");
        
        // COUNT(UNIQUE ?var)
        assertValidSyntax("SELECT COUNT(UNIQUE ?person) FROM documents WHERE CONTAINS(\"test\")");
        
        // COUNT(DOCUMENTS)
        assertValidSyntax("SELECT COUNT(DOCUMENTS) FROM documents WHERE CONTAINS(\"test\")");
        
        // Multiple count expressions
        assertValidSyntax("SELECT COUNT(*), COUNT(DOCUMENTS) FROM documents WHERE CONTAINS(\"test\")");
    }

    @Test
    @DisplayName("Test WHERE clause with various conditions")
    void testWhereClauseConditions() {
        // NER condition
        assertValidSyntax("SELECT ?person FROM documents WHERE NER(\"PERSON\", ?person)");
        
        // NER with wildcard
        assertValidSyntax("SELECT ?entity FROM documents WHERE NER(\"*\", ?entity)");
        
        // CONTAINS condition
        assertValidSyntax("SELECT ?doc FROM documents WHERE CONTAINS(\"artificial intelligence\")");
        
        // CONTAINS with multiple terms
        assertValidSyntax("SELECT ?doc FROM documents WHERE CONTAINS(\"AI\", \"machine learning\", \"neural network\")");
        
        // DATE condition with comparison operators
        assertValidSyntax("SELECT ?doc FROM documents WHERE DATE(?doc, > 1990)");
        assertValidSyntax("SELECT ?doc FROM documents WHERE DATE(?doc, < 2000)");
        assertValidSyntax("SELECT ?doc FROM documents WHERE DATE(?doc, >= 1995)");
        assertValidSyntax("SELECT ?doc FROM documents WHERE DATE(?doc, <= 2005)");
        assertValidSyntax("SELECT ?doc FROM documents WHERE DATE(?doc, == 2000)");
        
        // DATE condition with date operators
        assertValidSyntax("SELECT ?doc FROM documents WHERE DATE(?doc, CONTAINS [1990, 2000])");
        assertValidSyntax("SELECT ?doc FROM documents WHERE DATE(?doc, CONTAINED_BY 2000)");
        assertValidSyntax("SELECT ?doc FROM documents WHERE DATE(?doc, INTERSECT [1990, 2000])");
        assertValidSyntax("SELECT ?doc FROM documents WHERE DATE(?doc, NEAR 2000 RADIUS 5y)");
        
        // DEPENDS condition
        assertValidSyntax("SELECT ?subject FROM documents WHERE DEPENDS(?subject, \"nsubj\", \"eats\")");
    }

    @Test
    @DisplayName("Test AND operator and parenthesized conditions")
    void testAndOperatorAndParentheses() {
        // Simple AND
        assertValidSyntax("SELECT ?person FROM documents WHERE NER(\"PERSON\", ?person) AND CONTAINS(\"scientist\")");
        
        // Multiple ANDs
        assertValidSyntax("SELECT ?person FROM documents WHERE NER(\"PERSON\", ?person) AND CONTAINS(\"scientist\") AND DATE(?person, > 1900)");
        
        // Parenthesized conditions
        assertValidSyntax("SELECT ?person FROM documents WHERE (NER(\"PERSON\", ?person) AND CONTAINS(\"scientist\"))");
        
        // Nested parentheses
        assertValidSyntax("SELECT ?person FROM documents WHERE (NER(\"PERSON\", ?person) AND (CONTAINS(\"scientist\") AND DATE(?person, > 1900)))");
    }

    @Test
    @DisplayName("Test GRANULARITY clause")
    void testGranularityClause() {
        // DOCUMENT granularity
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\") GRANULARITY DOCUMENT");
        
        // SENTENCE granularity without size
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\") GRANULARITY SENTENCE");
        
        // SENTENCE granularity with size
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\") GRANULARITY SENTENCE 3");
    }

    @Test
    @DisplayName("Test ORDER BY clause")
    void testOrderByClause() {
        // Simple ORDER BY
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\") ORDER BY ?person");
        
        // ORDER BY with direction
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\") ORDER BY ?person ASC");
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\") ORDER BY ?person DESC");
        
        // Multiple ORDER BY fields
        assertValidSyntax("SELECT ?person, ?date FROM documents WHERE CONTAINS(\"test\") ORDER BY ?person ASC, ?date DESC");
    }

    @Test
    @DisplayName("Test LIMIT clause")
    void testLimitClause() {
        // Simple LIMIT
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\") LIMIT 10");
        
        // LIMIT with other clauses
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\") ORDER BY ?person LIMIT 10");
        assertValidSyntax("SELECT ?person FROM documents WHERE CONTAINS(\"test\") GRANULARITY SENTENCE 3 LIMIT 10");
    }

    @Test
    @DisplayName("Test combined features in complex queries")
    void testComplexQueries() {
        // Query with all features
        assertValidSyntax(
            "SELECT ?person, SNIPPET(?person, WINDOW=5), COUNT(UNIQUE ?org) " +
            "FROM documents " +
            "WHERE NER(\"PERSON\", ?person) AND NER(\"ORGANIZATION\", ?org) AND CONTAINS(\"founded\") " +
            "GRANULARITY SENTENCE 2 " +
            "ORDER BY ?person ASC, ?org DESC " +
            "LIMIT 20"
        );
        
        // Complex conditions with parentheses
        assertValidSyntax(
            "SELECT ?person " +
            "FROM documents " +
            "WHERE (NER(\"PERSON\", ?person) AND CONTAINS(\"physics\")) AND " +
            "(DATE(?person, > 1900) AND DATE(?person, < 2000)) " +
            "ORDER BY ?person " +
            "LIMIT 10"
        );
    }

    @Test
    @DisplayName("Test OR and NOT operators")
    void testOrAndNotOperators() {
        // Simple OR
        assertValidSyntax("SELECT ?person FROM documents WHERE NER(\"PERSON\", ?person) OR CONTAINS(\"scientist\")");
        
        // Multiple ORs
        assertValidSyntax("SELECT ?person FROM documents WHERE NER(\"PERSON\", ?person) OR CONTAINS(\"scientist\") OR DATE(?person, > 1900)");
        
        // Simple NOT
        assertValidSyntax("SELECT ?person FROM documents WHERE NOT NER(\"ORGANIZATION\", ?person)");
        
        // NOT with parentheses
        assertValidSyntax("SELECT ?person FROM documents WHERE NOT (NER(\"ORGANIZATION\", ?person))");
        
        // Mix of AND, OR, and NOT
        assertValidSyntax("SELECT ?person FROM documents WHERE NER(\"PERSON\", ?person) AND (CONTAINS(\"scientist\") OR CONTAINS(\"researcher\"))");
        assertValidSyntax("SELECT ?person FROM documents WHERE NER(\"PERSON\", ?person) OR (NOT CONTAINS(\"student\"))");
        
        // Complex logic with all operators
        assertValidSyntax(
            "SELECT ?person, ?org " +
            "FROM documents " +
            "WHERE (NER(\"PERSON\", ?person) OR NER(\"ORGANIZATION\", ?org)) AND " +
            "NOT (CONTAINS(\"irrelevant\") OR DATE(?person, < 1900))"
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        // Missing FROM
        "SELECT ?person WHERE CONTAINS(\"test\")",
        
        // Missing column in SELECT
        "SELECT FROM documents WHERE CONTAINS(\"test\")",
        
        // Incomplete WHERE
        "SELECT ?person FROM documents WHERE",
        
        // Missing operator in condition
        "SELECT ?person FROM documents WHERE NER(\"PERSON\", ?person) CONTAINS(\"scientist\")",
        
        // Invalid syntax in DATE comparison
        "SELECT ?person FROM documents WHERE DATE(?person, 1990)",
        
        // Invalid syntax in CONTAINS (missing parentheses)
        "SELECT ?person FROM documents WHERE CONTAINS \"test\"",
        
        // Invalid ORDER BY (missing field)
        "SELECT ?person FROM documents WHERE CONTAINS(\"test\") ORDER BY",
        
        // Invalid LIMIT (missing number)
        "SELECT ?person FROM documents WHERE CONTAINS(\"test\") LIMIT",
        
        // Invalid GRANULARITY
        "SELECT ?person FROM documents WHERE CONTAINS(\"test\") GRANULARITY PARAGRAPH"
    })
    @DisplayName("Test invalid syntax patterns")
    void testInvalidSyntaxPatterns(String invalidQuery) {
        assertInvalidSyntax(invalidQuery);
    }
} 