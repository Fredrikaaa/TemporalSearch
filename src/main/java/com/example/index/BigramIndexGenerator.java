package com.example.index;

import java.io.IOException;
import java.sql.Connection;
import java.util.*;

/**
 * Generates an index for word pairs (bigrams) from the annotations table.
 * Bigrams are formed from consecutive words within the same sentence.
 */
public class BigramIndexGenerator extends BaseIndexGenerator {
    private final Map<String, PositionList> bigramPositions;
    private static final int FLUSH_THRESHOLD = 10000;

    public BigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "bigram");
        this.bigramPositions = new HashMap<>();
    }

    @Override
    protected void processBatch(List<IndexEntry> entries) throws IOException {
        // Group entries by document and sentence
        Map<Integer, Map<Integer, List<IndexEntry>>> groupedEntries = new HashMap<>();

        for (IndexEntry entry : entries) {
            groupedEntries
                    .computeIfAbsent(entry.documentId, k -> new HashMap<>())
                    .computeIfAbsent(entry.sentenceId, k -> new ArrayList<>())
                    .add(entry);
        }

        // Process each document
        for (Map<Integer, List<IndexEntry>> docEntries : groupedEntries.values()) {
            // Process each sentence
            for (List<IndexEntry> sentenceEntries : docEntries.values()) {
                // Sort entries by position within sentence
                sentenceEntries.sort((a, b) -> Integer.compare(a.beginChar, b.beginChar));

                // Create bigrams from consecutive words
                for (int i = 0; i < sentenceEntries.size() - 1; i++) {
                    IndexEntry first = sentenceEntries.get(i);
                    IndexEntry second = sentenceEntries.get(i + 1);

                    // Skip if either word is null or empty
                    if (first.lemma == null || second.lemma == null ||
                            first.lemma.isEmpty() || second.lemma.isEmpty()) {
                        continue;
                    }

                    // Create bigram key
                    String bigramKey = createBigramKey(first.lemma, second.lemma);

                    // Create position spanning both words
                    Position position = new Position(
                            first.documentId,
                            first.sentenceId,
                            first.beginChar,
                            second.endChar,
                            first.timestamp);

                    // Add to bigram positions map
                    bigramPositions.computeIfAbsent(bigramKey, k -> new PositionList())
                            .add(position);
                }
            }
        }

        // If we've accumulated enough unique bigrams, flush to disk
        if (bigramPositions.size() >= FLUSH_THRESHOLD) {
            flushToDisk();
        }
    }

    /**
     * Creates a standardized key for storing bigrams.
     * Joins the lemmas with a delimiter that won't appear in the text.
     */
    private String createBigramKey(String lemma1, String lemma2) {
        return lemma1.toLowerCase() + "\u0000" + lemma2.toLowerCase();
    }

    /**
     * Writes accumulated bigram positions to LevelDB and clears the map
     */
    private void flushToDisk() throws IOException {
        for (Map.Entry<String, PositionList> entry : bigramPositions.entrySet()) {
            String bigram = entry.getKey();
            PositionList positions = entry.getValue();

            // Sort positions before serializing
            positions.sort();

            // Check if this bigram already has entries in LevelDB
            byte[] existingData = levelDb.get(bytes(bigram));
            if (existingData != null) {
                // Merge with existing positions
                PositionList existingPositions = PositionList.deserialize(existingData);
                positions.merge(existingPositions);
                positions.sort();
            }

            // Write to LevelDB
            writeToLevelDb(bigram, positions.serialize());
        }

        // Clear the map for next batch
        bigramPositions.clear();
    }

    @Override
    public void close() throws IOException {
        // Ensure any remaining positions are written to disk
        if (!bigramPositions.isEmpty()) {
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
