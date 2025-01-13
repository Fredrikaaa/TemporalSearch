package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;

/**
 * Generates unigram indexes from annotated text.
 * Each unigram (single word) is stored with its positions in the corpus.
 */
public class UnigramIndexGenerator extends BaseIndexGenerator {
    
    public UnigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "unigrams");
    }

    public UnigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "unigrams", threadCount);
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<IndexEntry> partition) throws IOException {
        if (partition == null) {
            throw new IOException("Partition cannot be null");
        }
        
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        // Track unique positions to avoid duplicates from overlapping partitions
        Set<String> processedPositions = new HashSet<>();
        
        for (IndexEntry entry : partition) {
            if (entry.lemma == null || isStopword(entry.lemma)) {
                continue;
            }

            // Create a unique key for this position
            String positionKey = String.format("%d-%d-%d-%d", 
                entry.documentId, entry.sentenceId, entry.beginChar, entry.endChar);
            
            // Skip if we've already processed this position (from overlap)
            if (!processedPositions.add(positionKey)) {
                continue;
            }

            String key = entry.lemma.toLowerCase();
            Position position = new Position(entry.documentId, entry.sentenceId, 
                                          entry.beginChar, entry.endChar, entry.timestamp);

            // Get or create position list for this term
            List<PositionList> lists = index.get(key);
            if (lists.isEmpty()) {
                PositionList newList = new PositionList();
                newList.add(position);
                index.put(key, newList);
            } else {
                lists.get(0).add(position);
            }
        }
        
        return index;
    }
}
