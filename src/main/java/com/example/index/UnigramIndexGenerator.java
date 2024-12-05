package com.example.index;

import java.io.IOException;
import java.sql.Connection;
import java.util.*;

/**
 * Generates an index for single words (unigrams) from the annotations table.
 */
public class UnigramIndexGenerator extends BaseIndexGenerator {
    private final Map<String, PositionList> wordPositions;
    private static final int FLUSH_THRESHOLD = 10000; // Number of unique words before flushing to disk

    public UnigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "unigram");
        this.wordPositions = new HashMap<>();
    }

    @Override
    protected void processBatch(List<IndexEntry> entries) throws IOException {
        // Process each entry in the batch
        for (IndexEntry entry : entries) {
            // Skip stopwords and empty/null lemmas
            if (isStopword(entry.lemma)) {
                continue;
            }

            // Create position object
            Position position = new Position(
                    entry.documentId,
                    entry.sentenceId,
                    entry.beginChar,
                    entry.endChar,
                    entry.timestamp);

            // Add to word positions map
            wordPositions.computeIfAbsent(entry.lemma, k -> new PositionList())
                    .add(position);
        }

        // If we've accumulated enough unique words, flush to disk
        if (wordPositions.size() >= FLUSH_THRESHOLD) {
            flushToDisk();
        }
    }

    /**
     * Writes accumulated word positions to LevelDB and clears the map
     */
    private void flushToDisk() throws IOException {
        for (Map.Entry<String, PositionList> entry : wordPositions.entrySet()) {
            String word = entry.getKey();
            PositionList positions = entry.getValue();

            // Sort positions before serializing
            positions.sort();

            // Check if this word already has entries in LevelDB
            byte[] existingData = levelDb.get(bytes(word));
            if (existingData != null) {
                // Merge with existing positions
                PositionList existingPositions = PositionList.deserialize(existingData);
                positions.merge(existingPositions);
                positions.sort();
            }

            // Write to LevelDB
            writeToLevelDb(word, positions.serialize());
        }

        // Clear the map for next batch
        wordPositions.clear();
    }

    @Override
    public void close() throws IOException {
        // Ensure any remaining positions are written to disk
        if (!wordPositions.isEmpty()) {
            flushToDisk();
        }
        super.close();
    }

    /**
     * Utility method for LevelDB serialization
     */
    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
