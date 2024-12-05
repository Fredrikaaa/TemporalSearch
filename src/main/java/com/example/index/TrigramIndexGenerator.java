package com.example.index;

import java.io.IOException;
import java.sql.Connection;
import java.util.*;

/**
 * Generates an index for word triplets (trigrams) from the annotations table.
 * Trigrams are formed from three consecutive words within the same sentence.
 */
public class TrigramIndexGenerator extends BaseIndexGenerator {
    private final Map<String, PositionList> trigramPositions;
    // Reduce threshold since trigrams will take more memory per entry
    private static final int FLUSH_THRESHOLD = 5000;

    public TrigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "trigram");
        this.trigramPositions = new HashMap<>();
    }

    @Override
    protected void processBatch(List<IndexEntry> entries) throws IOException {
        // Group entries by document and sentence for efficient processing
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

                // Create trigrams from consecutive words
                for (int i = 0; i < sentenceEntries.size() - 2; i++) {
                    IndexEntry first = sentenceEntries.get(i);
                    IndexEntry second = sentenceEntries.get(i + 1);
                    IndexEntry third = sentenceEntries.get(i + 2);

                    // Skip if any word is null or empty
                    if (first.lemma == null || second.lemma == null || third.lemma == null ||
                            first.lemma.isEmpty() || second.lemma.isEmpty() || third.lemma.isEmpty()) {
                        continue;
                    }

                    // Create trigram key using two delimiters
                    String trigramKey = createTrigramKey(first.lemma, second.lemma, third.lemma);

                    // Create position spanning all three words
                    Position position = new Position(
                            first.documentId,
                            first.sentenceId,
                            first.beginChar, // Start from first word
                            third.endChar, // End at third word
                            first.timestamp);

                    // Add to trigram positions map
                    trigramPositions.computeIfAbsent(trigramKey, k -> new PositionList())
                            .add(position);
                }
            }
        }

        // If we've accumulated enough unique trigrams, flush to disk
        if (trigramPositions.size() >= FLUSH_THRESHOLD) {
            flushToDisk();
        }
    }

    /**
     * Creates a standardized key for storing trigrams.
     * Joins the lemmas with a delimiter that won't appear in the text.
     */
    private String createTrigramKey(String lemma1, String lemma2, String lemma3) {
        return lemma1.toLowerCase() + "\u0000" + lemma2.toLowerCase() + "\u0000" + lemma3.toLowerCase();
    }

    /**
     * Writes accumulated trigram positions to LevelDB and clears the map
     */
    private void flushToDisk() throws IOException {
        for (Map.Entry<String, PositionList> entry : trigramPositions.entrySet()) {
            String trigram = entry.getKey();
            PositionList positions = entry.getValue();

            // Sort positions before serializing
            positions.sort();

            // Check if this trigram already has entries in LevelDB
            byte[] existingData = levelDb.get(bytes(trigram));
            if (existingData != null) {
                // Merge with existing positions
                PositionList existingPositions = PositionList.deserialize(existingData);
                positions.merge(existingPositions);
                positions.sort();
            }

            // Write to LevelDB
            writeToLevelDb(trigram, positions.serialize());
        }

        // Clear the map for next batch
        trigramPositions.clear();
    }

    @Override
    public void close() throws IOException {
        // Ensure any remaining positions are written to disk
        if (!trigramPositions.isEmpty()) {
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
