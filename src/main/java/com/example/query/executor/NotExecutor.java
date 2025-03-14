package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Not;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for NOT conditions.
 * Handles negation by executing the inner condition and inverting the result.
 */
public final class NotExecutor implements ConditionExecutor<Not> {
    private static final Logger logger = LoggerFactory.getLogger(NotExecutor.class);
    
    private final ConditionExecutorFactory executorFactory;
    
    /**
     * Creates a new NotConditionExecutor that uses the provided factory to create
     * executors for the inner condition.
     *
     * @param executorFactory The factory to use for creating condition executors
     */
    public NotExecutor(ConditionExecutorFactory executorFactory) {
        this.executorFactory = executorFactory;
    }
    
    @Override
    public Set<DocSentenceMatch> execute(Not condition, Map<String, IndexAccess> indexes,
                               VariableBindings variableBindings, Query.Granularity granularity,
                               int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing NOT condition at {} granularity with size {}", 
                granularity, granularitySize);
        
        // Execute inner condition
        Condition innerCondition = condition.condition();
        ConditionExecutor<Condition> executor = executorFactory.getExecutor(innerCondition);
        Set<DocSentenceMatch> innerMatches = executor.execute(
            innerCondition, indexes, variableBindings, granularity, granularitySize);
        
        Condition negatedCondition = condition.condition();
        logger.debug("Executing NOT operation on condition: {} with granularity: {} and size: {}", 
                    negatedCondition, granularity, granularitySize);
        
        try {
            // Get all document/sentence IDs from the collection
            Set<DocSentenceMatch> allMatches = getAllMatches(indexes, granularity);
            
            // Apply negation using the appropriate strategy
            Set<DocSentenceMatch> result = negateMatches(innerMatches, allMatches);
            
            logger.debug("NOT operation completed with {} matching results at {} granularity", 
                        result.size(), granularity);
            return result;
        } catch (Exception e) {
            if (e instanceof QueryExecutionException) {
                throw (QueryExecutionException) e;
            }
            throw new QueryExecutionException(
                "Error executing NOT condition: " + e.getMessage(),
                e,
                condition.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Negates a set of matches against a universe set.
     * Returns all matches from the universe that are not in the set to negate.
     *
     * @param toNegate The set of matches to negate
     * @param allMatches The universe of all possible matches
     * @return All matches from universe that are not in toNegate
     */
    Set<DocSentenceMatch> negateMatches(Set<DocSentenceMatch> toNegate, Set<DocSentenceMatch> allMatches) {
        if (toNegate.isEmpty()) {
            return allMatches;
        }

        Set<DocSentenceMatch> result = new HashSet<>();

        if (toNegate.iterator().next().isSentenceLevel()) {
            // Sentence-level negation
            Map<SentenceKey, DocSentenceMatch> toNegateMap = new HashMap<>();
            
            // Index matches to negate by sentence key
            for (DocSentenceMatch match : toNegate) {
                SentenceKey key = new SentenceKey(match.documentId(), match.sentenceId());
                toNegateMap.put(key, match);
            }
            
            // Add matches that don't appear in toNegate
            for (DocSentenceMatch match : allMatches) {
                SentenceKey key = new SentenceKey(match.documentId(), match.sentenceId());
                if (!toNegateMap.containsKey(key)) {
                    result.add(match);
                }
            }
        } else {
            // Document-level negation
            Set<Integer> toNegateIds = new HashSet<>();
            for (DocSentenceMatch match : toNegate) {
                toNegateIds.add(match.documentId());
            }
            
            // Add matches from documents not in toNegate
            for (DocSentenceMatch match : allMatches) {
                if (!toNegateIds.contains(match.documentId())) {
                    result.add(match);
                }
            }
        }

        return result;
    }
    
    /**
     * Gets all document/sentence matches in the collection.
     * This is used to compute the complement of a set for NOT operations.
     *
     * @param indexes The indexes to use
     * @param granularity Whether to return document or sentence level matches
     * @return Set of all matches at the specified granularity level
     * @throws QueryExecutionException If there's an error retrieving matches
     */
    private Set<DocSentenceMatch> getAllMatches(Map<String, IndexAccess> indexes, Query.Granularity granularity)
        throws QueryExecutionException {
        
        logger.debug("Retrieving all matches at {} granularity for NOT operation", granularity);
        
        // Use the metadata index if available, or any other index
        IndexAccess metadataIndex = indexes.get("metadata");
        
        if (metadataIndex == null) {
            // If no metadata index, try to use any index
            if (indexes.isEmpty()) {
                throw new QueryExecutionException(
                    "No indexes available to determine matches for NOT operation",
                    "NOT condition",
                    QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR
                );
            }
            
            // Use first available index
            metadataIndex = indexes.values().iterator().next();
            logger.debug("No metadata index available, using {} index instead", metadataIndex.getIndexType());
        }
        
        try {
            Set<DocSentenceMatch> allMatches = new HashSet<>();
            
            try (var iterator = metadataIndex.iterator()) {
                iterator.seekToFirst();
                
                while (iterator.hasNext()) {
                    byte[] keyBytes = iterator.peekNext().getKey();
                    String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                    
                    // Try to parse document ID from the key
                    // This depends on how document IDs are stored in the index
                    try {
                        int docId = Integer.parseInt(key);
                        
                        if (granularity == Query.Granularity.DOCUMENT) {
                            // For document granularity, just add the document
                            allMatches.add(new DocSentenceMatch(docId));
                        } else {
                            // For sentence granularity, we need to add all sentences in the document
                            // In a real implementation, we'd need to get the actual sentence count
                            // for each document from the index
                            int sentenceCount = getSentenceCount(metadataIndex, docId);
                            for (int sentenceId = 0; sentenceId < sentenceCount; sentenceId++) {
                                allMatches.add(new DocSentenceMatch(docId, sentenceId));
                            }
                        }
                    } catch (NumberFormatException e) {
                        // If the key isn't a document ID, skip it
                    }
                    
                    iterator.next();
                }
            }
            
            logger.debug("Found {} total matches at {} granularity", allMatches.size(), granularity);
            return allMatches;
        } catch (Exception e) {
            throw new QueryExecutionException(
                "Error retrieving all matches: " + e.getMessage(),
                e,
                "NOT condition",
                QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR
            );
        }
    }
    
    /**
     * Gets the number of sentences in a document.
     * This is a placeholder implementation - in a real system, this would
     * retrieve the actual sentence count from the index.
     *
     * @param index The index to use
     * @param documentId The document ID
     * @return The number of sentences in the document
     */
    private int getSentenceCount(IndexAccess index, int documentId) {
        // This is a placeholder implementation
        // In a real system, this would retrieve the actual sentence count from the index
        // For now, we'll assume a default of 10 sentences per document
        return 10;
    }
    
    /**
     * Helper class for sentence identification
     */
    private record SentenceKey(int documentId, int sentenceId) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SentenceKey that = (SentenceKey) o;
            return documentId == that.documentId && sentenceId == that.sentenceId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(documentId, sentenceId);
        }
    }
} 