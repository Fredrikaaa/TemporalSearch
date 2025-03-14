package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.anyLong;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.logging.ProgressTracker;
import com.example.core.IndexAccess;
import com.example.core.PositionList;
import com.example.index.StitchEntry;
import com.example.index.StitchIndexGenerator;
import com.example.index.StitchPosition;

@ExtendWith(MockitoExtension.class)
class StitchIndexGeneratorTest extends BaseIndexTest {
    private static final Logger logger = LoggerFactory.getLogger(StitchIndexGeneratorTest.class);
    private Path stopwordsPath;
    private StitchIndexGenerator generator;

    @Mock
    private ProgressTracker progress;

    @BeforeEach
    @Override
    void setUp() throws Exception {
        // Call parent setup
        super.setUp();
        
        // Create stopwords path
        stopwordsPath = tempDir.resolve("stopwords.txt");

        // Create empty stopwords file
        Files.writeString(stopwordsPath, "the\na\nan\n");

        // Create generator with new index base directory
        generator = new StitchIndexGenerator(
            indexBaseDir.toString(),
            stopwordsPath.toString(),
            sqliteConn,
            progress
        );
    }

    @AfterEach
    void cleanUpGenerator() throws Exception {
        // Call parent tearDown 
        if (generator != null) {
            try {
                generator.close();
                logger.info("Closed generator in cleanUpGenerator");
            } catch (Exception e) {
                logger.warn("Error closing generator: {}", e.getMessage());
            } finally {
                generator = null;
            }
        }
    }

    private void insertBasicTestData() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Insert document
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-03-20')");
            
            // Insert unigram annotations
            stmt.execute("""
                INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma, pos)
                VALUES (1, 1, 0, 7, 'company', 'company', 'NN')
            """);
            
            // Insert date annotations
            stmt.execute("""
                INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, ner, normalized_ner)
                VALUES (1, 1, 10, 20, 'March 2024', 'DATE', '2024-03-01')
            """);
        }
    }

    @Test
    void testFetchBatch() throws SQLException {
        // Insert test data
        insertBasicTestData();

        // Fetch batch
        List<StitchEntry> entries = generator.fetchBatch(0);

        // Verify results
        assertEquals(1, entries.size(), "Expected one stitch entry");
        
        // Verify stitch entry 
        StitchEntry entry = entries.get(0);
        
        assertEquals(1, entry.documentId());
        assertEquals(1, entry.sentenceId());
        // The beginChar and endChar should span from the unigram to the date
        assertEquals(0, entry.beginChar());
        assertEquals(20, entry.endChar());
        assertEquals("company", entry.value());
        assertEquals(LocalDate.parse("2024-03-20"), entry.timestamp());
        // The synonymId should be 1 (first ID assigned by DateSynonyms)
        assertEquals(1, entry.synonymId());
    }

    @Test
    void testProcessBatch() throws IOException {
        // Create test entries - note that each entry has a unigram value and date synonymId
        List<StitchEntry> batch = List.of(
            new StitchEntry(1, 1, 0, 20, LocalDate.parse("2024-03-20"), "company", 1),
            new StitchEntry(1, 1, 25, 45, LocalDate.parse("2024-03-20"), "founded", 2)
        );

        // Process batch
        var result = generator.processBatch(batch);

        // Verify results
        assertEquals(2, result.keySet().size(), "Expected entries for both unigrams");
        assertTrue(result.containsKey("company"));
        assertTrue(result.containsKey("founded"));

        // Verify positions for 'company' including associated date
        var companyPositions = result.get("company").get(0);
        assertEquals(1, companyPositions.getPositions().size(), "Expected one position for company");
        
        var companyPos = companyPositions.getPositions().get(0);
        assertEquals(0, companyPos.getBeginPosition());
        assertEquals(20, companyPos.getEndPosition());
        assertEquals(1, ((StitchPosition)companyPos).getSynonymId());
        
        // Verify positions for 'founded' including associated date
        var foundedPositions = result.get("founded").get(0);
        assertEquals(1, foundedPositions.getPositions().size(), "Expected one position for founded");
        
        var foundedPos = foundedPositions.getPositions().get(0);
        assertEquals(25, foundedPos.getBeginPosition());
        assertEquals(45, foundedPos.getEndPosition());
        assertEquals(2, ((StitchPosition)foundedPos).getSynonymId());
    }

    @Test
    void testGenerateIndex() throws Exception {
        // Insert test data
        insertBasicTestData();

        try {
            // Generate index
            generator.generateIndex();
            
            // Close the generator to release the LevelDB lock before creating the IndexAccess
            generator.close();
            generator = null;
            
            // Create a new IndexAccess instance for verification
            try (IndexAccess indexAccess = new IndexAccess(indexBaseDir, "stitch", createTestOptions())) {
                // Verify index contents for 'company'
                var companyPositions = indexAccess.get("company".getBytes());
                assertTrue(companyPositions.isPresent(), "Expected positions for 'company'");
                
                PositionList positions = companyPositions.get();
                assertEquals(1, positions.getPositions().size(), "Expected one stitch position");
                
                // Verify the stitch position properties
                var pos = positions.getPositions().get(0);
                // The position should span from the start of unigram to the end of date
                assertEquals(0, pos.getBeginPosition());
                assertEquals(20, pos.getEndPosition());
                
                // Verify the synonym ID is correctly associated
                if (pos instanceof StitchPosition) {
                    StitchPosition stitchPos = (StitchPosition) pos;
                    assertEquals(1, stitchPos.getSynonymId(), "Expected synonym ID 1 for 2024-03-01");
                    logger.info("Found StitchPosition with synonymId: {}", stitchPos.getSynonymId());
                } else {
                    fail("Position is not a StitchPosition, but a " + pos.getClass().getSimpleName());
                }
            }
    
            // Verify progress tracking
            verify(progress, atLeastOnce()).updateIndex(anyLong());
        } finally {
            // Ensure generator is closed in case of exception
            if (generator != null) {
                generator.close();
                generator = null;
            }
        }
    }
} 