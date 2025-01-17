package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;

/**
 * Generates bigram indexes from annotated text.
 * Each bigram (pair of consecutive words) is stored with its positions in the corpus.
 */
public class BigramIndexGenerator extends BaseIndexGenerator {
    
    public BigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "bigrams");
    }

    public BigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "bigrams", threadCount);
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<IndexEntry> partition) {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        Map<String, PositionList> positionLists = new HashMap<>();
        IndexEntry prevEntry = null;
        
        for (int i = 0; i < partition.size(); i++) {
            IndexEntry entry = partition.get(i);
            
            // Skip only if entry has null lemma
            if (entry.lemma == null) {
                prevEntry = null;
                continue;
            }
            
            // Process bigram with previous entry if they're from the same sentence
            if (prevEntry != null && 
                prevEntry.documentId == entry.documentId && 
                prevEntry.sentenceId == entry.sentenceId) {
                
                String key = String.format("%s%s%s", 
                    prevEntry.lemma.toLowerCase(), 
                    BaseIndexGenerator.DELIMITER, 
                    entry.lemma.toLowerCase());

                Position position = new Position(entry.documentId, entry.sentenceId,
                    prevEntry.beginChar, entry.endChar, entry.timestamp);

                // Get or create position list for this bigram
                PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
                posList.add(position);
            }
            
            // If this is the last entry and there's a next entry that could form a bigram
            if (i == partition.size() - 1 && i + 1 < partition.size()) {
                IndexEntry nextEntry = partition.get(i + 1);
                if (nextEntry.lemma != null &&
                    entry.documentId == nextEntry.documentId && 
                    entry.sentenceId == nextEntry.sentenceId) {
                    
                    String key = String.format("%s%s%s", 
                        entry.lemma.toLowerCase(), 
                        BaseIndexGenerator.DELIMITER, 
                        nextEntry.lemma.toLowerCase());

                    Position position = new Position(nextEntry.documentId, nextEntry.sentenceId,
                        entry.beginChar, nextEntry.endChar, nextEntry.timestamp);

                    PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
                    posList.add(position);
                }
            }
            
            prevEntry = entry;
        }
        
        // Add all position lists to result
        for (Map.Entry<String, PositionList> entry : positionLists.entrySet()) {
            index.put(entry.getKey(), entry.getValue());
        }
        
        return index;
    }
}
