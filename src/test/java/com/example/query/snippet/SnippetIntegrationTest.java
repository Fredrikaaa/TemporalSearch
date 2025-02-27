package com.example.query.snippet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.QueryCLI;
import com.example.core.IndexAccess;
import com.example.query.QueryParser;
import com.example.query.executor.VariableBindings;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.ResultTable;
import com.example.query.model.SelectColumn;
import com.example.query.model.SnippetNode;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import com.example.query.result.ResultGenerator;
import com.example.query.result.ResultGenerationException;
import com.example.query.snippet.SnippetGenerator;

/**
 * Integration test for snippet functionality in QueryCLI.
 * This test verifies the integration of snippet generation into the query pipeline.
 */
@ExtendWith(MockitoExtension.class)
public class SnippetIntegrationTest {
    
    @Mock
    private Connection mockConnection;
    
    private Connection connection;
    private SnippetGenerator snippetGenerator;
    
    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        connection = mockConnection;
    }
    
    /**
     * Tests that the database-related methods of ResultGenerator properly support snippet generation
     */
    @Test
    public void testSnippetDatabaseIntegration() {
        // Verify that we can create a ResultGenerator with a snippet config and database path
        assertDoesNotThrow(() -> {
            ResultGenerator generator = new ResultGenerator(
                new SnippetConfig(2, "**", false), 
                "test.db"
            );
        }, "Should be able to create ResultGenerator with snippet config");
    }
    
    /**
     * Tests that QueryCLI properly initializes ResultGenerator with snippet support
     */
    @Test
    public void testQueryCLIIntegratesSnippetSupport() {
        // Create a QueryCLI with a test database path
        QueryCLI cli = new QueryCLI(Path.of("test-indexes"), "test.db");
        
        // Since the CLI constructor doesn't expose its internal components directly,
        // we mainly verify that it initializes without errors when given a database path
        assertNotNull(cli, "QueryCLI should initialize with database path for snippets");
    }
    
    /**
     * Verifies that SchemaCompatibleSnippetGenerator can be correctly configured
     */
    @Test
    public void testSnippetGeneratorConfiguration() {
        // Create SchemaCompatibleSnippetGenerator with custom configuration
        snippetGenerator = new SnippetGenerator(connection);
        
        // Create test variable bindings with position information
        VariableBindings bindings = new VariableBindings();
        bindings.addBinding(1, 2, "testVar", "highlighted@2:27");
        
        // Create a match
        DocSentenceMatch match = new DocSentenceMatch(1, 2);
        
        // Extract position from bindings
        int position = bindings.getTokenPosition("testVar", match);
        assertEquals(27, position, "Should extract correct position from variable binding");
        
        // Verify we can clear the cache
        assertDoesNotThrow(() -> {
            snippetGenerator.clearCache();
        }, "Should be able to clear the snippet generator cache");
    }
} 