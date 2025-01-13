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
        IndexEntry prevPrevEntry = null;
        IndexEntry prevEntry = null;
        
        for (int i = 0; i < partition.size(); i++) {
            IndexEntry entry = partition.get(i);
            
            // Skip if entry has null lemma (but not if it's a stopword)
            if (entry.lemma == null) {
                prevPrevEntry = null;
                prevEntry = null;
                continue;
            }
            
            // Process trigram with previous entries if they're from the same sentence
            if (prevPrevEntry != null && prevEntry != null && 
                prevPrevEntry.documentId == entry.documentId && 
                prevPrevEntry.sentenceId == entry.sentenceId) {
                
                String key = String.format("%s\u0000%s\u0000%s", 
                    prevPrevEntry.lemma.toLowerCase(),
                    prevEntry.lemma.toLowerCase(),
                    entry.lemma.toLowerCase());

                Position position = new Position(entry.documentId, entry.sentenceId,
                    prevPrevEntry.beginChar, entry.endChar, entry.timestamp);

                // Get or create position list for this trigram
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
            
            // If this is the second-to-last or last entry, check for potential trigrams with next entries
            if ((i == partition.size() - 2 || i == partition.size() - 1) && 
                i + 2 < partition.size()) {
                IndexEntry nextEntry = partition.get(i + 1);
                IndexEntry nextNextEntry = partition.get(i + 2);
                
                // Check if we can form a trigram with the next two entries
                if (nextEntry.lemma != null && nextNextEntry.lemma != null &&
                    entry.documentId == nextEntry.documentId && 
                    entry.documentId == nextNextEntry.documentId &&
                    entry.sentenceId == nextEntry.sentenceId &&
                    entry.sentenceId == nextNextEntry.sentenceId) {
                    
                    String key = String.format("%s\u0000%s\u0000%s", 
                        entry.lemma.toLowerCase(),
                        nextEntry.lemma.toLowerCase(),
                        nextNextEntry.lemma.toLowerCase());

                    Position position = new Position(nextNextEntry.documentId, nextNextEntry.sentenceId,
                        entry.beginChar, nextNextEntry.endChar, nextNextEntry.timestamp);

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
            
            prevPrevEntry = prevEntry;
            prevEntry = entry;
        }
        
        return index;
    }
}
