package com.example.query.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for visualizing parse trees.
 * This helps in understanding how the ANTLR parser interprets different query patterns.
 * The tests in this class are not meant to be assertions but rather tools for debugging
 * and understanding the grammar.
 */
@DisplayName("Parse Tree Visualization Tests")
public class ParseTreeVisualizationTest {

    /**
     * Parses a query string using the ANTLR parser and returns the parse tree.
     */
    private ParseTree parseQuery(String queryString) {
        CharStream input = CharStreams.fromString(queryString);
        QueryLangLexer lexer = new QueryLangLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        QueryLangParser parser = new QueryLangParser(tokens);
        return parser.query();
    }

    /**
     * Generates a textual representation of the parse tree.
     */
    private String getParseTreeText(ParseTree tree) {
        StringBuilder sb = new StringBuilder();
        parseTreeToString(tree, 0, sb);
        return sb.toString();
    }

    private void parseTreeToString(ParseTree tree, int level, StringBuilder sb) {
        // Indent based on level
        sb.append("  ".repeat(level));
        
        if (tree instanceof TerminalNode) {
            Token token = ((TerminalNode) tree).getSymbol();
            int tokenType = token.getType();
            String tokenName = QueryLangLexer.VOCABULARY.getSymbolicName(tokenType);
            sb.append(tokenName).append(":'").append(token.getText()).append("'");
        } else {
            String ruleName = QueryLangParser.ruleNames[((RuleContext) tree).getRuleIndex()];
            sb.append(ruleName);
        }
        
        sb.append("\n");
        
        // Process all children
        for (int i = 0; i < tree.getChildCount(); i++) {
            parseTreeToString(tree.getChild(i), level + 1, sb);
        }
    }

    /**
     * Visualizes the parse tree for a sample query.
     * This test doesn't assert anything - it just logs the structure for inspection.
     */
    @Test
    @DisplayName("Visualize parse tree for sample query")
    public void testVisualizeParseTree() {
        String query = "SELECT ?person, SNIPPET(?person, WINDOW=5) " +
                      "FROM corpus " +
                      "WHERE NER(\"PERSON\", ?person) AND DATE(?person, > 2000) " +
                      "GRANULARITY SENTENCE 3 " +
                      "ORDER BY ?person DESC " +
                      "LIMIT 5";
        
        ParseTree tree = parseQuery(query);
        String treeString = getParseTreeText(tree);
        
        // Print the parse tree structure (for debugging & understanding)
        System.out.println("Parse tree for: " + query);
        System.out.println("=".repeat(80));
        System.out.println(treeString);
        System.out.println("=".repeat(80));
        
        // No assertions - this test is for visualization purposes only
    }

    /**
     * Visualizes a parse tree specifically for examining date expressions.
     */
    @Test
    @DisplayName("Visualize parse tree for date expressions")
    public void testVisualizeDateExpressions() {
        String[] queries = {
            "SELECT ?doc FROM corpus WHERE DATE(?doc, > 1990)",
            "SELECT ?doc FROM corpus WHERE DATE(?doc, CONTAINS [1990, 2000])",
            "SELECT ?doc FROM corpus WHERE DATE(?doc, NEAR 2000 RADIUS 5y)"
        };
        
        for (String query : queries) {
            ParseTree tree = parseQuery(query);
            String treeString = getParseTreeText(tree);
            
            System.out.println("Parse tree for: " + query);
            System.out.println("=".repeat(80));
            System.out.println(treeString);
            System.out.println("=".repeat(80));
        }
    }

    /**
     * Visualizes a parse tree specifically for examining count expressions.
     */
    @Test
    @DisplayName("Visualize parse tree for count expressions")
    public void testVisualizeCountExpressions() {
        String[] queries = {
            "SELECT COUNT(*) FROM corpus WHERE CONTAINS(\"neural network\")",
            "SELECT COUNT(UNIQUE ?person) FROM corpus WHERE NER(\"PERSON\", ?person)",
            "SELECT COUNT(DOCUMENTS) FROM corpus WHERE CONTAINS(\"machine learning\")"
        };
        
        for (String query : queries) {
            ParseTree tree = parseQuery(query);
            String treeString = getParseTreeText(tree);
            
            System.out.println("Parse tree for: " + query);
            System.out.println("=".repeat(80));
            System.out.println(treeString);
            System.out.println("=".repeat(80));
        }
    }
} 