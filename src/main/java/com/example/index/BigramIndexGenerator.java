package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;

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
        
        for (IndexEntry entry : partition) {
            if (prevEntry != null && 
                prevEntry.documentId == entry.documentId && 
                prevEntry.sentenceId == entry.sentenceId &&
                !isStopword(prevEntry.lemma) && !isStopword(entry.lemma)) {
                
                String key = String.format("%s %s", 
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
                    lists.get(0).add(position);
                }
            }
            prevEntry = entry;
        }
        
        return index;
    }
}
