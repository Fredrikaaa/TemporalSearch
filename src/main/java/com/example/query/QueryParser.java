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
            if (msg.contains("extraneous input '}'")) {
                throw new UnsupportedOperationException("Subqueries are not yet supported");
            }
            throw new RuntimeException("Syntax error at position " + charPositionInLine + ": " + msg);
        }
    }

    /**
     * Parses a query string and returns a Query object.
     *
     * @param queryString The query string to parse
     * @return The parsed Query object
     * @throws QueryParseException if there is an error parsing the query
     * @throws UnsupportedOperationException if a feature is not yet implemented
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
        } catch (UnsupportedOperationException e) {
            // Pass through UnsupportedOperationException
            throw e;
        } catch (RuntimeException e) {
            logger.error("Error parsing query: {}", queryString, e);
            String message = e.getMessage();
            if (message != null && message.startsWith("Failed to parse query: ")) {
                message = message.substring("Failed to parse query: ".length());
            }
            throw new QueryParseException(message, e);
        }
    }
} 