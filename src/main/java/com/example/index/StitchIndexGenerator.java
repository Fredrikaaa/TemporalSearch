package com.example.index;

import com.example.logging.ProgressTracker;
import com.example.core.PositionList;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

/**
 * Generates a stitch index that connects unigrams with their associated dates.
 * This index enables efficient querying of temporal relationships between words and dates
 * in the document collection.
 */
public class StitchIndexGenerator extends IndexGenerator<StitchEntry> {
    private static final Logger logger = LoggerFactory.getLogger(StitchIndexGenerator.class);
    private final DateSynonyms dateSynonyms;
    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    public StitchIndexGenerator(String indexBaseDir, String stopwordsPath,
            java.sql.Connection sqliteConn, ProgressTracker progress) throws IOException {
        super(indexBaseDir, stopwordsPath, sqliteConn, progress);
        Path baseDir = Path.of(indexBaseDir);
        this.dateSynonyms = new DateSynonyms(baseDir);
        try {
            logger.info("Initializing date synonyms for stitch index");
            populateDateSynonyms();
            logger.info("Successfully initialized date synonyms with {} entries", dateSynonyms.size());
        } catch (SQLException e) {
            // Close resources to prevent memory leaks
            try {
                dateSynonyms.close();
            } catch (Exception ex) {
                logger.warn("Failed to close date synonyms after initialization error", ex);
            }
            throw new IOException("Failed to populate date synonyms", e);
        }
    }

    public StitchIndexGenerator(String indexBaseDir, String stopwordsPath,
            java.sql.Connection sqliteConn, ProgressTracker progress, IndexConfig config) throws IOException {
        super(indexBaseDir, stopwordsPath, sqliteConn, progress, config);
        Path baseDir = Path.of(indexBaseDir);
        this.dateSynonyms = new DateSynonyms(baseDir);
        try {
            logger.info("Initializing date synonyms for stitch index with custom config");
            populateDateSynonyms();
            logger.info("Successfully initialized date synonyms with {} entries", dateSynonyms.size());
        } catch (SQLException e) {
            // Close resources to prevent memory leaks
            try {
                dateSynonyms.close();
            } catch (Exception ex) {
                logger.warn("Failed to close date synonyms after initialization error", ex);
            }
            throw new IOException("Failed to populate date synonyms", e);
        }
    }

    /**
     * Populates the date synonyms from the database.
     * Fetches all unique normalized date entities and creates mappings for them.
     */
    private void populateDateSynonyms() throws SQLException, IOException {
        String query = """
            SELECT DISTINCT normalized_ner
            FROM annotations
            WHERE ner = 'DATE'
                AND normalized_ner IS NOT NULL
            ORDER BY normalized_ner
        """;

        int count = 0;
        try (Statement stmt = sqliteConn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                String dateValue = rs.getString(1);
                if (dateValue != null && DATE_PATTERN.matcher(dateValue).matches()) {
                    try {
                        dateSynonyms.getOrCreateId(dateValue); // This will create if not exists
                        count++;
                    } catch (IllegalArgumentException e) {
                        logger.warn("Skipping invalid date format: {}", dateValue, e);
                    }
                }
            }
        }
        
        // Validate synonyms to ensure consistency
        dateSynonyms.validateSynonyms();
        logger.info("Populated {} date synonyms", count);
    }

    @Override
    protected List<StitchEntry> fetchBatch(int offset) throws SQLException {
        // Simplified query that finds unigrams and dates within the same sentence
        String query = """
            WITH sentence_dates AS (
                SELECT
                    document_id,
                    sentence_id,
                    begin_char as date_begin,
                    end_char as date_end,
                    normalized_ner as date_value
                FROM annotations
                WHERE 
                    ner = 'DATE' 
                    AND normalized_ner IS NOT NULL
            ),
            sentence_unigrams AS (
                SELECT
                    document_id,
                    sentence_id,
                    begin_char as unigram_begin,
                    end_char as unigram_end,
                    token as unigram_value,
                    lemma
                FROM annotations
                WHERE 
                    pos NOT IN ('PUNCT', 'SYM')
                    AND token IS NOT NULL
                    AND LENGTH(token) > 0
            )
            SELECT
                u.document_id,
                u.sentence_id,
                u.unigram_begin,
                u.unigram_end,
                u.unigram_value,
                u.lemma,
                d.date_begin,
                d.date_end,
                d.date_value,
                doc.timestamp
            FROM 
                sentence_unigrams u
            JOIN 
                sentence_dates d ON u.document_id = d.document_id AND u.sentence_id = d.sentence_id
            JOIN 
                documents doc ON u.document_id = doc.document_id
            ORDER BY u.document_id, u.sentence_id, u.unigram_begin
            LIMIT ? OFFSET ?
        """;

        List<StitchEntry> entries = new ArrayList<>();
        int skippedEntries = 0;
        
        try (PreparedStatement stmt = sqliteConn.prepareStatement(query)) {
            stmt.setInt(1, config.getBatchSize());
            stmt.setInt(2, offset);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    // Get date value and verify it matches our pattern
                    String dateValue = rs.getString("date_value");
                    if (dateValue == null || !DATE_PATTERN.matcher(dateValue).matches()) {
                        skippedEntries++;
                        continue;
                    }
                    
                    try {
                        // Get the unigram value and normalize to lowercase
                        String unigramValue = rs.getString("unigram_value");
                        if (unigramValue == null) {
                            skippedEntries++;
                            continue;
                        }
                        
                        // Skip stopwords
                        if (isStopword(unigramValue)) {
                            continue;
                        }
                        
                        // Skip if the unigram is the date itself or part of it
                        // This avoids self-referential date stitches
                        int unigramBegin = rs.getInt("unigram_begin");
                        int unigramEnd = rs.getInt("unigram_end");
                        int dateBegin = rs.getInt("date_begin");
                        int dateEnd = rs.getInt("date_end");
                        
                        // Skip if unigram contains the date (date position is inside unigram position)
                        if (unigramBegin <= dateBegin && unigramEnd >= dateEnd) {
                            logger.debug("Skipping self-referential date stitch: '{}' contains '{}'", 
                                      unigramValue, dateValue);
                            continue;
                        }
                        
                        // Skip if unigram is contained by the date
                        if (dateBegin <= unigramBegin && dateEnd >= unigramEnd) {
                            logger.debug("Skipping self-referential date stitch: '{}' is contained in '{}'", 
                                      unigramValue, dateValue);
                            continue;
                        }
                        
                        // Skip if the unigram is the date or contains the date string
                        String unigramLower = unigramValue.toLowerCase();
                        String dateLower = dateValue.toLowerCase();
                        if (unigramLower.equals(dateLower) || 
                            unigramLower.contains(dateLower) || 
                            dateLower.contains(unigramLower)) {
                            logger.debug("Skipping self-referential date stitch: '{}' and '{}' are textually related", 
                                      unigramValue, dateValue);
                            continue;
                        }
                        
                        // Convert unigram to lowercase for consistent lookup
                        unigramValue = unigramValue.toLowerCase();
                        
                        // Get the synonym ID from the DateSynonyms lookup
                        int synonymId = dateSynonyms.getOrCreateId(dateValue);
                        
                        // Create stitch position that spans from the earliest position to the latest
                        int beginChar = Math.min(unigramBegin, dateBegin);
                        int endChar = Math.max(unigramEnd, dateEnd);
                        
                        // Parse the document timestamp
                        String timestamp = rs.getString("timestamp");
                        LocalDate docDate = LocalDate.parse(timestamp.substring(0, 10));
                        
                        // Log the association being created
                        logger.debug("Creating stitch entry: unigram='{}', date='{}', synonymId={}", 
                                    unigramValue, dateValue, synonymId);
                        
                        // Create a StitchEntry with the unigram value as the key and proper synonym ID
                        entries.add(new StitchEntry(
                            rs.getInt("document_id"),
                            rs.getInt("sentence_id"),
                            beginChar,
                            endChar,
                            docDate,
                            unigramValue,  // Lowercase unigram value
                            synonymId      // Store the date's synonym ID
                        ));
                    } catch (IllegalArgumentException e) {
                        logger.warn("Skipping invalid date '{}' for synonym creation: {}", 
                                   dateValue, e.getMessage());
                        skippedEntries++;
                    } catch (Exception e) {
                        logger.error("Failed to process stitch entry for date {}: {}", 
                                    dateValue, e.getMessage());
                        skippedEntries++;
                    }
                }
            }
        }
        
        if (skippedEntries > 0) {
            logger.info("Skipped {} entries with invalid dates or self-referential stitches in this batch", skippedEntries);
        }
        
        logger.debug("Fetched {} stitch entries at offset {}", entries.size(), offset);
        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processBatch(List<StitchEntry> batch) {
        ListMultimap<String, PositionList> index = ArrayListMultimap.create();
        
        // Group entries by unigram value
        Map<String, List<StitchEntry>> groupedEntries = batch.stream()
            .collect(Collectors.groupingBy(StitchEntry::value));
        
        // Process each unigram group
        for (Map.Entry<String, List<StitchEntry>> entry : groupedEntries.entrySet()) {
            String unigram = entry.getKey();
            List<StitchEntry> entries = entry.getValue();
            
            // Create position list with stitch positions
            PositionList positions = new PositionList();
            for (StitchEntry e : entries) {
                // Add position with the synonym ID from the date
                StitchPosition position = new StitchPosition(
                    e.documentId(), 
                    e.sentenceId(),
                    e.beginChar(), 
                    e.endChar(),
                    e.timestamp(), 
                    e.synonymId()  // This is the date synonym ID
                );
                
                positions.add(position);
            }
            
            index.put(unigram, positions);
        }
        
        logger.debug("Processed batch with {} unique unigrams", index.keySet().size());
        return index;
    }

    @Override
    protected String getTableName() {
        return "annotations";
    }

    @Override
    protected String getIndexName() {
        return "stitch";
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing StitchIndexGenerator and associated resources");
        Exception firstException = null;
        
        try {
            dateSynonyms.close();
        } catch (Exception e) {
            firstException = e;
            logger.error("Error closing date synonyms", e);
        }
        
        try {
            super.close();
        } catch (Exception e) {
            if (firstException == null) {
                throw e;
            } else {
                logger.error("Error closing index generator after previous error", e);
                // Add the second exception as a suppressed exception to the first
                firstException.addSuppressed(e);
                throw new IOException("Multiple errors occurred during close", firstException);
            }
        }
        
        if (firstException != null) {
            throw new IOException("Error closing date synonyms", firstException);
        }
        
        logger.info("Successfully closed StitchIndexGenerator");
    }
} 