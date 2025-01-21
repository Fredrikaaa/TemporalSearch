package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ListMultimap;

class HypernymIndexGeneratorTest {
    @TempDir
    Path tempDir;
    
    @Mock
    Connection sqliteConn;
    
    @Mock
    PreparedStatement stmt;
    
    @Mock
    ResultSet rs;
    
    private Path levelDbPath;
    private Path stopwordsPath;
    private HypernymIndexGenerator generator;
    private static final String TEST_TIMESTAMP = "2024-01-21T12:00:00Z";
    private static final LocalDate TEST_DATE = LocalDate.parse("2024-01-21");
    
    @BeforeEach
    void setUp() throws IOException, SQLException {
        MockitoAnnotations.openMocks(this);
        levelDbPath = tempDir.resolve("leveldb");
        stopwordsPath = createStopwordsFile();
        
        // Create test instance
        generator = new HypernymIndexGenerator(
            levelDbPath.toString(),
            stopwordsPath.toString(),
            100, // batch size
            sqliteConn,
            "annotations",
            1  // single thread for testing
        );
    }
    
    private Path createStopwordsFile() throws IOException {
        Path stopwordsFile = tempDir.resolve("stopwords.txt");
        List<String> stopwords = List.of("the", "a", "an");
        Files.write(stopwordsFile, stopwords);
        return stopwordsFile;
    }
    
    @Test
    void testCreateKey() {
        String key = generator.createKey("Animals", "Cat");
        assertEquals("animals" + BaseIndexGenerator.DELIMITER + "cat", key);
    }
    
    @Test
    void testCreateKeyWithMixedCase() {
        String key = generator.createKey("ANIMALS", "CAT");
        assertEquals("animals" + BaseIndexGenerator.DELIMITER + "cat", key);
    }
    
    @Test
    void testProcessPartitionWithEmptyList() {
        List<IndexEntry> emptyPartition = new ArrayList<>();
        ListMultimap<String, PositionList> result = generator.processPartition(emptyPartition);
        assertTrue(result.isEmpty());
    }

    @Test
    void testProcessHypernymRelation() throws SQLException {
        // Mock document timestamp query
        PreparedStatement timestampStmt = mock(PreparedStatement.class);
        ResultSet timestampRs = mock(ResultSet.class);
        when(sqliteConn.prepareStatement(contains("SELECT timestamp"))).thenReturn(timestampStmt);
        when(timestampStmt.executeQuery()).thenReturn(timestampRs);
        when(timestampRs.next()).thenReturn(true);
        when(timestampRs.getString("timestamp")).thenReturn(TEST_TIMESTAMP);

        // Mock dependencies query
        PreparedStatement depsStmt = mock(PreparedStatement.class);
        ResultSet depsRs = mock(ResultSet.class);
        when(sqliteConn.prepareStatement(contains("SELECT d.*"))).thenReturn(depsStmt);
        when(depsStmt.executeQuery()).thenReturn(depsRs);
        
        // Mock one hypernym relation
        when(depsRs.next()).thenReturn(true, false);
        when(depsRs.getString("relation")).thenReturn("nmod:such_as");
        when(depsRs.getString("head_lemma")).thenReturn("animal");
        when(depsRs.getString("dependent_lemma")).thenReturn("cat");
        when(depsRs.getInt("sentence_id")).thenReturn(1);
        when(depsRs.getInt("begin_char")).thenReturn(10);
        when(depsRs.getInt("end_char")).thenReturn(20);

        // Process a single document
        IndexEntry entry = new IndexEntry(1, 1, 10, 20, "animal", "nmod:such_as", TEST_DATE);
        List<IndexEntry> partition = List.of(entry);
        ListMultimap<String, PositionList> result = generator.processPartition(partition);

        // Verify results
        assertFalse(result.isEmpty());
        String expectedKey = "animal" + BaseIndexGenerator.DELIMITER + "cat";
        assertTrue(result.containsKey(expectedKey));
        
        // Verify position details
        List<PositionList> positions = result.get(expectedKey);
        assertFalse(positions.isEmpty());
        PositionList posList = positions.get(0);
        assertEquals(1, posList.size());
        Position pos = posList.getPositions().get(0);
        assertEquals(1, pos.getDocumentId());
        assertEquals(1, pos.getSentenceId());
        assertEquals(10, pos.getBeginPosition());
        assertEquals(20, pos.getEndPosition());
        assertEquals(TEST_DATE, pos.getTimestamp());
    }

    @Test
    void testSkipStopwords() throws SQLException {
        // Mock document timestamp query
        PreparedStatement timestampStmt = mock(PreparedStatement.class);
        ResultSet timestampRs = mock(ResultSet.class);
        when(sqliteConn.prepareStatement(contains("SELECT timestamp"))).thenReturn(timestampStmt);
        when(timestampStmt.executeQuery()).thenReturn(timestampRs);
        when(timestampRs.next()).thenReturn(true);
        when(timestampRs.getString("timestamp")).thenReturn(TEST_TIMESTAMP);

        // Mock dependencies query
        PreparedStatement depsStmt = mock(PreparedStatement.class);
        ResultSet depsRs = mock(ResultSet.class);
        when(sqliteConn.prepareStatement(contains("SELECT d.*"))).thenReturn(depsStmt);
        when(depsStmt.executeQuery()).thenReturn(depsRs);
        
        // Mock relation with stopword
        when(depsRs.next()).thenReturn(true, false);
        when(depsRs.getString("relation")).thenReturn("nmod:such_as");
        when(depsRs.getString("head_lemma")).thenReturn("the");  // stopword
        when(depsRs.getString("dependent_lemma")).thenReturn("cat");

        // Process a single document
        IndexEntry entry = new IndexEntry(1, 1, 10, 20, "the", "nmod:such_as", TEST_DATE);
        List<IndexEntry> partition = List.of(entry);
        ListMultimap<String, PositionList> result = generator.processPartition(partition);

        // Verify stopword pair was skipped
        assertTrue(result.isEmpty());
    }
} 