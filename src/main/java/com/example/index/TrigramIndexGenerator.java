package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;

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
        
        for (IndexEntry entry : partition) {
            if (prevPrevEntry != null && prevEntry != null && 
                prevPrevEntry.documentId == entry.documentId && 
                prevPrevEntry.sentenceId == entry.sentenceId &&
                !isStopword(prevPrevEntry.lemma) && 
                !isStopword(prevEntry.lemma) && 
                !isStopword(entry.lemma)) {
                
                String key = String.format("%s %s %s", 
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
                    lists.get(0).add(position);
                }
            }
            prevPrevEntry = prevEntry;
            prevEntry = entry;
        }
        
        return index;
    }
}
