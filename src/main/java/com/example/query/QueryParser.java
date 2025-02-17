package com.example.query;

import com.example.query.model.Query;
import com.example.query.parser.QueryModelBuilder;
import com.example.query.parser.QueryLangLexer;
import com.example.query.parser.QueryLangParser;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for parsing queries.
 * This class hides the ANTLR implementation details from the rest of the application.
 */
public class QueryParser {
    private static final Logger logger = LoggerFactory.getLogger(QueryParser.class);

    private static class ThrowingErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
            throw new RuntimeException("Syntax error at position " + charPositionInLine + ": " + msg);
        }
    }

    /**
     * Parses a query string and returns a Query object.
     *
     * @param queryString The query string to parse
     * @return The parsed Query object
     * @throws QueryParseException if there is an error parsing the query
     */
    public Query parse(String queryString) throws QueryParseException {
        try {
            logger.debug("Parsing query: {}", queryString);

            // Create the lexer and parser
            QueryLangLexer lexer = new QueryLangLexer(CharStreams.fromString(queryString));
            lexer.removeErrorListeners();
            lexer.addErrorListener(new ThrowingErrorListener());

            CommonTokenStream tokens = new CommonTokenStream(lexer);
            QueryLangParser parser = new QueryLangParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(new ThrowingErrorListener());

            // Parse the query
            ParseTree tree = parser.query();

            // Check for syntax errors
            if (parser.getNumberOfSyntaxErrors() > 0) {
                throw new QueryParseException("Invalid query syntax");
            }

            // Convert the parse tree to our model objects
            QueryModelBuilder visitor = new QueryModelBuilder();
            Query query = visitor.buildQuery(tree);

            logger.debug("Successfully parsed query: {}", query);
            return query;
        } catch (RuntimeException e) {
            logger.error("Error parsing query: {}", queryString, e);
            throw new QueryParseException("Failed to parse query: " + e.getMessage(), e);
        }
    }
} 