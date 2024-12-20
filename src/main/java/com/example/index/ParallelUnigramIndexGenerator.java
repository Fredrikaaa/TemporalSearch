package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;

/**
 * Parallel implementation of unigram index generation.
 * Processes documents in parallel to create a unigram index.
 */
public class ParallelUnigramIndexGenerator extends ParallelIndexGenerator {
    
    public ParallelUnigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "unigrams", threadCount);
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<IndexEntry> partition) {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        
        for (IndexEntry entry : partition) {
            if (entry.lemma == null || isStopword(entry.lemma)) {
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