package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.ArrayListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.io.IOException;

public class DependencyIndexGenerator extends BaseIndexGenerator {
    private static final Logger logger = LoggerFactory.getLogger(DependencyIndexGenerator.class);
    
    private static final Set<String> BLACKLISTED_RELATIONS = new HashSet<>(Arrays.asList(
        "punct", "det", "case", "cc"
    ));

    public DependencyIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "dependencies");
    }

    public DependencyIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "dependencies", threadCount);
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<IndexEntry> entries) {
        ListMultimap<String, PositionList> result = ArrayListMultimap.create();
        Map<String, PositionList> positionLists = new HashMap<>();

        // Group entries by document and sentence
        Map<Integer, Map<Integer, List<IndexEntry>>> docSentMap = new HashMap<>();
        for (IndexEntry entry : entries) {
            docSentMap
                .computeIfAbsent(entry.documentId, k -> new HashMap<>())
                .computeIfAbsent(entry.sentenceId, k -> new ArrayList<>())
                .add(entry);
        }

        // Process each document
        for (Map.Entry<Integer, Map<Integer, List<IndexEntry>>> docEntry : docSentMap.entrySet()) {
            int docId = docEntry.getKey();
            
            // Process each sentence
            for (Map.Entry<Integer, List<IndexEntry>> sentEntry : docEntry.getValue().entrySet()) {
                int sentId = sentEntry.getKey();
                List<IndexEntry> sentEntries = sentEntry.getValue();
                
                // Create dependency pairs for each word combination
                for (IndexEntry headEntry : sentEntries) {
                    for (IndexEntry depEntry : sentEntries) {
                        if (headEntry == depEntry) {
                            continue;
                        }

                        // Skip if either word is a stopword
                        if (isStopword(headEntry.lemma) || isStopword(depEntry.lemma)) {
                            continue;
                        }

                        // Create the key with a generic "dep" relation
                        String key = String.format("%s\u0000%s\u0000%s",
                            headEntry.lemma.toLowerCase(),
                            "dep",
                            depEntry.lemma.toLowerCase());
                        
                        // Log position information for debugging
                        logger.debug("Creating dependency pair: {} -> {}, chars: ({}-{}) -> ({}-{})",
                            headEntry.lemma, depEntry.lemma, headEntry.beginChar, headEntry.endChar,
                            depEntry.beginChar, depEntry.endChar);
                        
                        // Use the minimum and maximum character positions to span both tokens
                        int beginPos = Math.min(headEntry.beginChar, depEntry.beginChar);
                        int endPos = Math.max(headEntry.endChar, depEntry.endChar);
                        
                        Position pos = new Position(
                            docId,
                            sentId,
                            beginPos,
                            endPos,
                            headEntry.timestamp
                        );
                        
                        // Add to position list
                        PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
                        posList.add(pos);
                    }
                }
            }
        }
        
        // Add all position lists to result
        for (Map.Entry<String, PositionList> entry : positionLists.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        
        return result;
    }
} 