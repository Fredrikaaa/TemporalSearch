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
                
                String key = String.format("%s\u0000%s", 
                    prevEntry.lemma.toLowerCase(), 
                    entry.lemma.toLowerCase());

                Position position = new Position(entry.documentId, entry.sentenceId,
                    prevEntry.beginChar, entry.endChar, entry.timestamp);

                // Get or create position list for this bigram
                List<PositionList> lists = index.get(key);
                if (lists.isEmpty()) {
                    PositionList newList = new PositionList();
                    newList.add(position);
                    index.put(key, newList);
                } else {
                    // Check if this position is already recorded
                    boolean exists = false;
                    for (Position pos : lists.get(0).getPositions()) {
                        if (pos.getDocumentId() == position.getDocumentId() &&
                            pos.getSentenceId() == position.getSentenceId() &&
                            pos.getBeginPosition() == position.getBeginPosition() &&
                            pos.getEndPosition() == position.getEndPosition()) {
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) {
                        lists.get(0).add(position);
                    }
                }
            }
            
            // If this is the last entry and there's a next entry that could form a bigram
            if (i == partition.size() - 1 && i + 1 < partition.size()) {
                IndexEntry nextEntry = partition.get(i + 1);
                if (nextEntry.lemma != null &&
                    entry.documentId == nextEntry.documentId && 
                    entry.sentenceId == nextEntry.sentenceId) {
                    
                    String key = String.format("%s\u0000%s", 
                        entry.lemma.toLowerCase(), 
                        nextEntry.lemma.toLowerCase());

                    Position position = new Position(nextEntry.documentId, nextEntry.sentenceId,
                        entry.beginChar, nextEntry.endChar, nextEntry.timestamp);

                    List<PositionList> lists = index.get(key);
                    if (lists.isEmpty()) {
                        PositionList newList = new PositionList();
                        newList.add(position);
                        index.put(key, newList);
                    } else {
                        // Check if this position is already recorded
                        boolean exists = false;
                        for (Position pos : lists.get(0).getPositions()) {
                            if (pos.getDocumentId() == position.getDocumentId() &&
                                pos.getSentenceId() == position.getSentenceId() &&
                                pos.getBeginPosition() == position.getBeginPosition() &&
                                pos.getEndPosition() == position.getEndPosition()) {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists) {
                            lists.get(0).add(position);
                        }
                    }
                }
            }
            
            prevEntry = entry;
        }
        
        return index;
    }
}
