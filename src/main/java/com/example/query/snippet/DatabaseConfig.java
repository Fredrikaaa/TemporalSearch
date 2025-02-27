package com.example.query.snippet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration constants for database connections.
 */
public class DatabaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfig.class);
    /**
     * Default path to the SQLite database file.
     */
    public static final String DEFAULT_DB_PATH = "dump/wikipedia-cirrussearch-content.db";
    
    // Private constructor to prevent instantiation
    private DatabaseConfig() {}

    /**
     * Gets a connection to the database
     * @return Database connection
     * @throws SQLException if a database access error occurs
     */
    public static Connection getConnection() throws SQLException {
        return getConnection(DEFAULT_DB_PATH);
    }

    /**
     * Gets a connection to the specified database
     * @param dbPath Path to the database file
     * @return Database connection
     * @throws SQLException if a database access error occurs
     */
    public static Connection getConnection(String dbPath) throws SQLException {
        logger.debug("Opening connection to database: {}", dbPath);
        try {
            Class.forName("org.sqlite.JDBC");
            return DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        } catch (ClassNotFoundException e) {
            logger.error("SQLite JDBC driver not found", e);
            throw new SQLException("SQLite JDBC driver not found", e);
        }
    }
} 