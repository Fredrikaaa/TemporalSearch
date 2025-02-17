package com.example.query;

/**
 * Exception thrown when there is an error parsing a query.
 */
public class QueryParseException extends Exception {
    public QueryParseException(String message) {
        super(message);
    }

    public QueryParseException(String message, Throwable cause) {
        super(message, cause);
    }
} 