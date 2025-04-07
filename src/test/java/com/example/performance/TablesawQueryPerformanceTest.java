package com.example.performance;

import com.example.core.Position;
import com.example.query.model.DocSentenceMatch;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.tablesaw.api.*;
import tech.tablesaw.selection.Selection;
import tech.tablesaw.aggregate.AggregateFunctions;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Performance tests for evaluating Tablesaw as a potential replacement for
 * the current data structure in QueryExecutor.
 * 
 * This test creates a large Tablesaw table with several million rows and
 * performs operations that simulate typical query processing operations.
 * 
 * Note: This test is disabled by default as it can take significant time to run.
 * Enable it manually when you want to run the performance evaluation.
 */
@Disabled("Performance test - run manually")
public class TablesawQueryPerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(TablesawQueryPerformanceTest.class);
    
    // Constants for test data generation
    private static final int NUM_DOCS = 10_000;
    private static final int MAX_SENTENCES_PER_DOC = 50;
    private static final int NUM_KEYS = 5;
    private static final int MAX_POSITIONS_PER_KEY = 10;
    private static final LocalDate START_DATE = LocalDate.of(2020, 1, 1);
    private static final String[] SOURCES = {"wikipedia", "news", "academic", "social"};
    
    // Keep test datasets as static fields to avoid regenerating for each test
    private static Table largeTable;
    private static Set<DocSentenceMatch> largeMatchSet;
    
    @BeforeAll
    public static void setup() {
        // This setup is only performed once for all tests
        logger.info("Setting up test data...");
    }
    
    @ParameterizedTest
    @ValueSource(ints = {10_000, 100_000, 1_000_000})
    @DisplayName("Compare performance between Set<DocSentenceMatch> and Tablesaw")
    public void comparePerformance(int rowCount) {
        logger.info("Running performance test with {} rows", rowCount);
        
        // Generate test data for both approaches
        Set<DocSentenceMatch> matchSet = generateMatchSet(rowCount);
        Table table = convertToTablesaw(matchSet);
        
        logger.info("Generated {} matches and Tablesaw table with {} rows", 
                matchSet.size(), table.rowCount());
        
        // Benchmark typical operations
        runBenchmark("Filter by documentId", 
            () -> filterByDocumentIdSet(matchSet, 5000),
            () -> filterByDocumentIdTablesaw(table, 5000));
        
        runBenchmark("Filter by source", 
            () -> filterBySourceSet(matchSet, "wikipedia"),
            () -> filterBySourceTablesaw(table, "wikipedia"));
            
        runBenchmark("Join/Intersection operation", 
            () -> intersectSetsSet(matchSet),
            () -> intersectSetsTablesaw(table));
        
        runBenchmark("Group by document", 
            () -> groupByDocumentSet(matchSet),
            () -> groupByDocumentTablesaw(table));
    }
    
    @Test
    @DisplayName("Test memory usage of Tablesaw vs Set<DocSentenceMatch>")
    public void compareMemoryUsage() {
        // Force garbage collection
        System.gc();
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        
        // Create a large set of matches
        Set<DocSentenceMatch> matchSet = generateMatchSet(1_000_000);
        
        System.gc();
        long memAfterSet = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long setMemory = memAfterSet - memBefore;
        
        // Convert to Tablesaw
        Table table = convertToTablesaw(matchSet);
        
        System.gc();
        long memAfterTablesaw = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long tablesawMemory = memAfterTablesaw - memAfterSet;
        
        logger.info("Memory usage comparison:");
        logger.info("Set<DocSentenceMatch>: {} MB", setMemory / (1024 * 1024));
        logger.info("Tablesaw Table: {} MB", tablesawMemory / (1024 * 1024));
    }
    
    /**
     * Generate a set of DocSentenceMatch objects of specified size.
     */
    private Set<DocSentenceMatch> generateMatchSet(int size) {
        Set<DocSentenceMatch> matches = new HashSet<>();
        Random random = new Random(42); // Use fixed seed for reproducibility
        
        for (int i = 0; i < size; i++) {
            int docId = random.nextInt(NUM_DOCS);
            int sentId = random.nextInt(MAX_SENTENCES_PER_DOC);
            String source = SOURCES[random.nextInt(SOURCES.length)];
            
            // Either document or sentence level match
            DocSentenceMatch match = random.nextBoolean() 
                ? new DocSentenceMatch(docId, sentId, source)
                : new DocSentenceMatch(docId, source);
            
            // Add random positions for different keys
            for (int k = 0; k < NUM_KEYS; k++) {
                if (random.nextBoolean()) { // 50% chance to have this key
                    String key = "key" + k;
                    int numPositions = random.nextInt(MAX_POSITIONS_PER_KEY) + 1;
                    
                    for (int p = 0; p < numPositions; p++) {
                        int begin = random.nextInt(100);
                        int end = begin + random.nextInt(10) + 1;
                        LocalDate date = START_DATE.plus(random.nextInt(365 * 3), ChronoUnit.DAYS);
                        
                        Position position = new Position(docId, sentId, begin, end, date);
                        match.addPosition(key, position);
                    }
                }
            }
            
            // Add some variable bindings
            if (random.nextBoolean()) {
                match.setVariableValue("?var1", "value" + random.nextInt(100));
            }
            if (random.nextBoolean()) {
                match.setVariableValue("?var2", random.nextInt(1000));
            }
            
            matches.add(match);
            
            // Stop if we've reached the desired size
            if (matches.size() >= size) {
                break;
            }
        }
        
        return matches;
    }
    
    /**
     * Convert a set of DocSentenceMatch objects to a Tablesaw table.
     */
    private Table convertToTablesaw(Set<DocSentenceMatch> matches) {
        // Create columns for the main table
        IntColumn docIdColumn = IntColumn.create("document_id");
        IntColumn sentIdColumn = IntColumn.create("sentence_id");
        StringColumn sourceColumn = StringColumn.create("source");
        StringColumn levelColumn = StringColumn.create("level");
        
        // Create columns for variables (simplified - we'll just track if they exist)
        BooleanColumn hasVar1Column = BooleanColumn.create("has_var1");
        BooleanColumn hasVar2Column = BooleanColumn.create("has_var2");
        
        // Columns for tracking keys with positions
        BooleanColumn[] hasKeyColumns = new BooleanColumn[NUM_KEYS];
        IntColumn[] keyCountColumns = new IntColumn[NUM_KEYS];
        
        for (int i = 0; i < NUM_KEYS; i++) {
            hasKeyColumns[i] = BooleanColumn.create("has_key" + i);
            keyCountColumns[i] = IntColumn.create("key" + i + "_count");
        }
        
        // Add all rows
        for (DocSentenceMatch match : matches) {
            docIdColumn.append(match.documentId());
            sentIdColumn.append(match.sentenceId());
            sourceColumn.append(match.getSource());
            levelColumn.append(match.isSentenceLevel() ? "sentence" : "document");
            
            // Variable tracking
            hasVar1Column.append(match.getVariableValue("?var1") != null);
            hasVar2Column.append(match.getVariableValue("?var2") != null);
            
            // Position tracking
            for (int i = 0; i < NUM_KEYS; i++) {
                String key = "key" + i;
                Set<Position> positions = match.getPositions(key);
                hasKeyColumns[i].append(!positions.isEmpty());
                keyCountColumns[i].append(positions.size());
            }
        }
        
        // Build the table
        Table table = Table.create("matches");
        table.addColumns(docIdColumn, sentIdColumn, sourceColumn, levelColumn);
        table.addColumns(hasVar1Column, hasVar2Column);
        
        for (int i = 0; i < NUM_KEYS; i++) {
            table.addColumns(hasKeyColumns[i], keyCountColumns[i]);
        }
        
        return table;
    }
    
    /**
     * Run a benchmark comparing two implementations of the same operation.
     */
    private void runBenchmark(String name, Runnable setImpl, Runnable tablesawImpl) {
        logger.info("Benchmarking: {}", name);
        
        // Warm up
        for (int i = 0; i < 3; i++) {
            setImpl.run();
            tablesawImpl.run();
        }
        
        // Time Set implementation
        long setStart = System.nanoTime();
        setImpl.run();
        long setTime = System.nanoTime() - setStart;
        
        // Time Tablesaw implementation
        long tablesawStart = System.nanoTime();
        tablesawImpl.run();
        long tablesawTime = System.nanoTime() - tablesawStart;
        
        logger.info("{} - Set: {} ms, Tablesaw: {} ms", 
                name, 
                TimeUnit.NANOSECONDS.toMillis(setTime),
                TimeUnit.NANOSECONDS.toMillis(tablesawTime));
    }
    
    /**
     * Generic benchmark method that returns the result and timing.
     */
    private <T> Pair<T, Long> time(Supplier<T> operation) {
        long start = System.nanoTime();
        T result = operation.get();
        long duration = System.nanoTime() - start;
        return new Pair<>(result, duration);
    }
    
    // Record to hold a result and its execution time
    private record Pair<T, U>(T first, U second) {}
    
    // IMPLEMENTATION OF TYPICAL OPERATIONS
    // WITH BOTH SET<DocSentenceMatch> AND TABLESAW
    
    // 1. Filter by document ID
    private Set<DocSentenceMatch> filterByDocumentIdSet(Set<DocSentenceMatch> matches, int docId) {
        return matches.stream()
                .filter(m -> m.documentId() == docId)
                .collect(Collectors.toSet());
    }
    
    private Table filterByDocumentIdTablesaw(Table table, int docId) {
        Selection selection = table.intColumn("document_id").isEqualTo(docId);
        return table.where(selection);
    }
    
    // 2. Filter by source
    private Set<DocSentenceMatch> filterBySourceSet(Set<DocSentenceMatch> matches, String source) {
        return matches.stream()
                .filter(m -> source.equals(m.getSource()))
                .collect(Collectors.toSet());
    }
    
    private Table filterBySourceTablesaw(Table table, String source) {
        Selection selection = table.stringColumn("source").isEqualTo(source);
        return table.where(selection);
    }
    
    // 3. Intersection operation (simulating AND condition)
    private Set<DocSentenceMatch> intersectSetsSet(Set<DocSentenceMatch> matches) {
        // Split the set into two arbitrary subsets based on a condition
        Set<DocSentenceMatch> subset1 = matches.stream()
                .filter(m -> m.documentId() % 2 == 0)
                .collect(Collectors.toSet());
                
        Set<DocSentenceMatch> subset2 = matches.stream()
                .filter(m -> m.getSource().equals("wikipedia") || m.getSource().equals("news"))
                .collect(Collectors.toSet());
                
        // Compute intersection
        Set<DocSentenceMatch> result = new HashSet<>(subset1);
        result.retainAll(subset2);
        return result;
    }
    
    private Table intersectSetsTablesaw(Table table) {
        IntColumn docIdColumn = table.intColumn("document_id");
        StringColumn sourceColumn = table.stringColumn("source");
        
        // Create a list to hold the row indices that match both conditions
        List<Integer> matchingRowIndices = new ArrayList<>();
        
        // Find rows that match both conditions
        for (int i = 0; i < table.rowCount(); i++) {
            int docId = docIdColumn.get(i);
            String source = sourceColumn.get(i);
            
            if (docId % 2 == 0 && (source.equals("wikipedia") || source.equals("news"))) {
                matchingRowIndices.add(i);
            }
        }
        
        // Create a new table with only the matching rows
        return table.rows(matchingRowIndices.stream().mapToInt(i -> i).toArray());
    }
    
    // 4. Group by document
    private Map<Integer, List<DocSentenceMatch>> groupByDocumentSet(Set<DocSentenceMatch> matches) {
        return matches.stream()
                .collect(Collectors.groupingBy(DocSentenceMatch::documentId));
    }
    
    private Table groupByDocumentTablesaw(Table table) {
        return table.summarize(
                table.intColumn("sentence_id"), 
                table.intColumn("key0_count"),
                table.intColumn("key1_count"),
                AggregateFunctions.mean,
                AggregateFunctions.max,
                AggregateFunctions.count)
                .by(table.intColumn("document_id"));
    }
} 