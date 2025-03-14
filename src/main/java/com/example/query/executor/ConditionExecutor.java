package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Condition;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Interface for executing conditions against indexes.
 * Each condition type has a corresponding executor implementation.
 *
 * @param <T> The specific condition type this executor handles
 * @see com.example.query.model.condition.Condition
 */
public sealed interface ConditionExecutor<T extends Condition> 
    permits ContainsExecutor,
            DependencyExecutor,
            LogicalExecutor,
            NerExecutor,
            NotExecutor,
            PosExecutor,
            TemporalExecutor {
    
    /**
     * Executes a specific condition type against the appropriate indexes with a specified granularity window size
     *
     * @param condition The condition to execute
     * @param indexes Map of index name to IndexAccess
     * @param variableBindings Current variable bindings to update
     * @param granularity Whether to return document or sentence level matches
     * @param granularitySize Window size for sentence granularity (0 = same sentence only, 1 = adjacent sentences, etc.)
     * @return Set of matches at the specified granularity level
     * @throws QueryExecutionException if execution fails
     */
    Set<DocSentenceMatch> execute(T condition, Map<String, IndexAccess> indexes,
                         VariableBindings variableBindings, Query.Granularity granularity,
                         int granularitySize)
        throws QueryExecutionException;

    /**
     * Helper method to add a document-level match.
     */
    default void addDocumentMatch(Position position, Map<Integer, DocSentenceMatch> matches, String variableName) {
        int docId = position.getDocumentId();
        
        // Get or create document-level match
        DocSentenceMatch match = matches.computeIfAbsent(docId,
            id -> new DocSentenceMatch(docId));
        
        // Add position to match
        match.addPosition(variableName, position);
    }

    /**
     * Helper method to add a sentence-level match.
     */
    default void addSentenceMatch(Position position, Map<SentenceKey, DocSentenceMatch> matches, String variableName) {
        int docId = position.getDocumentId();
        int sentId = position.getSentenceId();
        
        // Get or create sentence-level match
        SentenceKey key = new SentenceKey(docId, sentId);
        DocSentenceMatch match = matches.computeIfAbsent(key,
            k -> new DocSentenceMatch(docId, sentId));
        
        // Add position to match
        match.addPosition(variableName, position);
    }

    /**
     * Helper method to add matches within a window around a sentence.
     */
    default void addWindowMatch(Position position, Map<SentenceKey, DocSentenceMatch> matches, String variableName, int windowSize) {
        int docId = position.getDocumentId();
        int sentenceId = position.getSentenceId();
        
        // Create matches for each sentence in the window
        for (int offset = -windowSize; offset <= windowSize; offset++) {
            int targetSentenceId = sentenceId + offset;
            if (targetSentenceId < 0) continue;
            
            // Get or create match for this sentence
            SentenceKey key = new SentenceKey(docId, targetSentenceId);
            DocSentenceMatch match = matches.computeIfAbsent(key,
                k -> new DocSentenceMatch(docId, targetSentenceId));
            
            // Add position to match
            match.addPosition(variableName, position);
        }
    }

    /**
     * Helper record for sentence identification.
     */
    record SentenceKey(int documentId, int sentenceId) {
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