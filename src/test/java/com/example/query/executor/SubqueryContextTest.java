package com.example.query.executor;

import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.SubquerySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SubqueryContextTest {

    private SubqueryContext context;
    private SubquerySpec subquery1;
    private SubquerySpec subquery2;
    private Set<DocSentenceMatch> results1;
    private Set<DocSentenceMatch> results2;
    private Table table1;
    private Table table2;

    @BeforeEach
    void setUp() {
        context = new SubqueryContext();
        
        // Create mock subqueries
        Query baseQuery1 = new Query("source1");
        Query baseQuery2 = new Query("source2");
        subquery1 = new SubquerySpec(baseQuery1, "sq1");
        subquery2 = new SubquerySpec(baseQuery2, "sq2", Optional.of(java.util.List.of("col1", "col2")));
        
        // Create mock results
        results1 = new HashSet<>();
        results1.add(new DocSentenceMatch(1, 1, "source1"));
        results1.add(new DocSentenceMatch(2, 2, "source1"));
        
        results2 = new HashSet<>();
        results2.add(new DocSentenceMatch(3, -1, "source2"));
        
        // Create mock tables
        table1 = Table.create("Table1");
        table1.addColumns(StringColumn.create("document_id", new String[]{"1", "2"}));
        table1.addColumns(StringColumn.create("sentence_id", new String[]{"1", "2"}));
        
        table2 = Table.create("Table2");
        table2.addColumns(StringColumn.create("document_id", new String[]{"3"}));
        table2.addColumns(StringColumn.create("col1", new String[]{"value1"}));
        table2.addColumns(StringColumn.create("col2", new String[]{"value2"}));
    }

    @Test
    void testAddAndGetNativeResults() {
        // Add results
        context.addNativeResults(subquery1, results1);
        
        // Verify results are stored correctly
        assertEquals(results1, context.getNativeResults("sq1"));
        assertNull(context.getNativeResults("sq2"));
        
        // Add more results
        context.addNativeResults(subquery2, results2);
        
        // Verify both results are available
        assertEquals(results1, context.getNativeResults("sq1"));
        assertEquals(results2, context.getNativeResults("sq2"));
    }

    @Test
    void testAddAndGetTableResults() {
        // Add tables
        context.addTableResults(subquery1, table1);
        
        // Verify tables are stored correctly
        assertEquals(table1, context.getTableResults("sq1"));
        assertNull(context.getTableResults("sq2"));
        
        // Add more tables
        context.addTableResults(subquery2, table2);
        
        // Verify both tables are available
        assertEquals(table1, context.getTableResults("sq1"));
        assertEquals(table2, context.getTableResults("sq2"));
    }

    @Test
    void testHasResults() {
        // Initially, no results
        assertFalse(context.hasResults("sq1"));
        assertFalse(context.hasResults("sq2"));
        
        // Add only native results for sq1
        context.addNativeResults(subquery1, results1);
        assertTrue(context.hasResults("sq1"));
        assertFalse(context.hasResults("sq2"));
        
        // Add only table results for sq2
        context.addTableResults(subquery2, table2);
        assertTrue(context.hasResults("sq1"));
        assertTrue(context.hasResults("sq2"));
    }

    @Test
    void testGetAliases() {
        // Initially, no aliases
        assertTrue(context.getAliases().isEmpty());
        
        // Add table results
        context.addTableResults(subquery1, table1);
        context.addTableResults(subquery2, table2);
        
        // Verify aliases
        assertEquals(2, context.getAliases().size());
        assertTrue(context.getAliases().contains("sq1"));
        assertTrue(context.getAliases().contains("sq2"));
    }

    @Test
    void testNullParameters() {
        // Test null parameters
        assertThrows(NullPointerException.class, () -> context.addNativeResults(null, results1));
        assertThrows(NullPointerException.class, () -> context.addNativeResults(subquery1, null));
        assertThrows(NullPointerException.class, () -> context.addTableResults(null, table1));
        assertThrows(NullPointerException.class, () -> context.addTableResults(subquery1, null));
    }
} 