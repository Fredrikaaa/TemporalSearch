package com.example.query.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import com.example.query.executor.strategies.NaiveContainsJoinStrategy;
import com.example.query.executor.strategies.NaiveProximityJoinStrategy;
import com.example.query.model.JoinCondition;
import com.example.query.model.TemporalPredicate;

/**
 * Tests for the JoinExecutor with multiple strategy implementations.
 */
public class JoinStrategyTest {
    
    private JoinExecutor joinExecutor;
    private Table leftTable;
    private Table rightTable;
    
    @BeforeEach
    public void setup() {
        // Create join executor
        joinExecutor = new JoinExecutor();
        
        // Register strategy implementations
        joinExecutor.registerStrategy(
                TemporalPredicate.CONTAINS, 
                "naive", 
                new NaiveContainsJoinStrategy(),
                true);
        
        joinExecutor.registerStrategy(
                TemporalPredicate.PROXIMITY, 
                "naive", 
                new NaiveProximityJoinStrategy(),
                true);
        
        // Create test tables
        createTestTables();
    }
    
    private void createTestTables() {
        // Create left table with event dates
        leftTable = Table.create("Events");
        StringColumn eventNameCol = StringColumn.create("event_name");
        DateColumn eventDateCol = DateColumn.create("event_date");
        
        eventNameCol.append("Conference A");
        eventDateCol.append(LocalDate.of(2023, 5, 15));
        
        eventNameCol.append("Conference B");
        eventDateCol.append(LocalDate.of(2023, 6, 20));
        
        eventNameCol.append("Workshop C");
        eventDateCol.append(LocalDate.of(2023, 7, 10));
        
        leftTable.addColumns(eventNameCol, eventDateCol);
        
        // Create right table with person availability
        rightTable = Table.create("Availability");
        StringColumn personNameCol = StringColumn.create("person_name");
        DateColumn availableDateCol = DateColumn.create("available_date");
        
        personNameCol.append("John");
        availableDateCol.append(LocalDate.of(2023, 5, 15));  // Exact match with Conference A
        
        personNameCol.append("Alice");
        availableDateCol.append(LocalDate.of(2023, 6, 21));  // 1 day after Conference B
        
        personNameCol.append("Bob");
        availableDateCol.append(LocalDate.of(2023, 7, 15));  // 5 days after Workshop C
        
        rightTable.addColumns(personNameCol, availableDateCol);
    }
    
    @Test
    public void testContainsJoin() throws Exception {
        // Create join condition for exact date match
        JoinCondition joinCondition = new JoinCondition(
                "event_date", 
                "available_date", 
                JoinCondition.JoinType.INNER, 
                TemporalPredicate.CONTAINS
        );
        
        // Execute join
        Table result = joinExecutor.join(leftTable, rightTable, joinCondition);
        
        // Verify result
        assertNotNull(result);
        assertEquals(1, result.rowCount());  // Only John on 2023-5-15 should match exactly
        assertEquals("Conference A", result.getString(0, "event_name"));
        assertEquals("John", result.getString(0, "person_name"));
        assertEquals(LocalDate.of(2023, 5, 15), result.dateColumn("event_date").get(0));
    }
    
    @Test
    public void testProximityJoin() throws Exception {
        // Create join condition with 2-day window
        JoinCondition joinCondition = new JoinCondition(
                "event_date", 
                "available_date", 
                JoinCondition.JoinType.INNER, 
                TemporalPredicate.PROXIMITY, 
                Optional.of(2)  // 2-day window
        );
        
        // Execute join
        Table result = joinExecutor.join(leftTable, rightTable, joinCondition);
        
        // Verify result
        assertNotNull(result);
        assertEquals(2, result.rowCount());  // John and Alice should match within 2 days
        
        // Check the results
        boolean foundJohn = false;
        boolean foundAlice = false;
        
        for (int i = 0; i < result.rowCount(); i++) {
            String eventName = result.getString(i, "event_name");
            String personName = result.getString(i, "person_name");
            
            if (eventName.equals("Conference A") && personName.equals("John")) {
                foundJohn = true;
            } else if (eventName.equals("Conference B") && personName.equals("Alice")) {
                foundAlice = true;
            }
        }
        
        assertTrue(foundJohn, "Should find John matching Conference A");
        assertTrue(foundAlice, "Should find Alice matching Conference B");
    }
    
    @Test
    public void testBenchmarkImplementations() throws Exception {
        // Register an alternative implementation (just using the same one for the test)
        joinExecutor.registerStrategy(
                TemporalPredicate.CONTAINS, 
                "alternative", 
                new NaiveContainsJoinStrategy(),
                false);
        
        // Create join condition
        JoinCondition joinCondition = new JoinCondition(
                "event_date", 
                "available_date", 
                JoinCondition.JoinType.INNER, 
                TemporalPredicate.CONTAINS
        );
        
        // Benchmark implementations
        Map<String, Long> benchmarkResults = joinExecutor.benchmark(leftTable, rightTable, joinCondition);
        
        // Verify results
        assertNotNull(benchmarkResults);
        assertEquals(2, benchmarkResults.size());  // "naive" and "alternative"
        assertTrue(benchmarkResults.containsKey("naive"));
        assertTrue(benchmarkResults.containsKey("alternative"));
        
        // Verify times are positive
        assertTrue(benchmarkResults.get("naive") >= 0);
        assertTrue(benchmarkResults.get("alternative") >= 0);
    }
    
    @Test
    public void testLeftJoin() throws Exception {
        // Create left join condition
        JoinCondition joinCondition = new JoinCondition(
                "event_date", 
                "available_date", 
                JoinCondition.JoinType.LEFT, 
                TemporalPredicate.CONTAINS
        );
        
        // Execute join
        Table result = joinExecutor.join(leftTable, rightTable, joinCondition);
        
        // Verify result
        assertNotNull(result);
        assertEquals(3, result.rowCount());  // All events should be included
        
        // Check that all event names are present
        boolean foundA = false;
        boolean foundB = false;
        boolean foundC = false;
        
        for (int i = 0; i < result.rowCount(); i++) {
            String eventName = result.getString(i, "event_name");
            
            if (eventName.equals("Conference A")) foundA = true;
            if (eventName.equals("Conference B")) foundB = true;
            if (eventName.equals("Workshop C")) foundC = true;
        }
        
        assertTrue(foundA && foundB && foundC, "All events should be included in LEFT join");
    }
} 