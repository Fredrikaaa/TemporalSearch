package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates indexes for part-of-speech tags from annotated text.
 * Each POS tag is stored with its positions in the corpus.
 * No stopword filtering is applied as all POS tags are considered significant.
 */
public class POSIndexGenerator extends BaseIndexGenerator {
    private static final Logger logger = LoggerFactory.getLogger(POSIndexGenerator.class);
    
    public POSIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "pos");
    }

    public POSIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "pos", threadCount);
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<IndexEntry> partition) {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (IndexEntry entry : partition) {
            // Skip entries with null POS tags
            if (entry.pos == null) {
                continue;
            }
            
            // Convert POS tag to lowercase for consistency
            String key = entry.pos.toLowerCase();
            
            Position position = new Position(entry.documentId, entry.sentenceId,
                entry.beginChar, entry.endChar, entry.timestamp);

            // Get or create position list for this POS tag
            PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
            posList.add(position);
        }
        
        // Add all position lists to result
        for (Map.Entry<String, PositionList> entry : positionLists.entrySet()) {
            index.put(entry.getKey(), entry.getValue());
        }
        
        return index;
    }
} 