package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;

/**
 * Generates trigram indexes from annotated text.
 * Each trigram (sequence of three consecutive words) is stored with its positions in the corpus.
 */
public class TrigramIndexGenerator extends BaseIndexGenerator {
    
    public TrigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "trigrams");
    }

    public TrigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "trigrams", threadCount);
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<IndexEntry> partition) {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        
        for (int i = 0; i < partition.size() - 2; i++) {
            IndexEntry entry = partition.get(i);
            IndexEntry nextEntry = partition.get(i + 1);
            IndexEntry nextNextEntry = partition.get(i + 2);
            
            // Skip if any entry has null lemma
            if (entry.lemma == null || nextEntry.lemma == null || nextNextEntry.lemma == null) {
                continue;
            }
            
            // Check if all entries are from the same document and sentence
            if (entry.documentId == nextEntry.documentId && 
                entry.documentId == nextNextEntry.documentId &&
                entry.sentenceId == nextEntry.sentenceId &&
                entry.sentenceId == nextNextEntry.sentenceId) {
                
                String key = String.format("%s%s%s%s%s", 
                    entry.lemma.toLowerCase(),
                    BaseIndexGenerator.DELIMITER,
                    nextEntry.lemma.toLowerCase(),
                    BaseIndexGenerator.DELIMITER,
                    nextNextEntry.lemma.toLowerCase());

                Position position = new Position(nextNextEntry.documentId, nextNextEntry.sentenceId,
                    entry.beginChar, nextNextEntry.endChar, nextNextEntry.timestamp);

                List<PositionList> lists = index.get(key);
                if (lists.isEmpty()) {
                    PositionList newList = new PositionList();
                    newList.add(position);
                    index.put(key, newList);
                } else {
                    lists.get(0).add(position);
                }
            }
        }
        
        return index;
    }
}
