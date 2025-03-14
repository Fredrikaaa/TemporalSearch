package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Logical.LogicalOperator;
import com.example.query.model.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for logical conditions (AND, OR).
 * Handles recursive execution and result combination of subconditions.
 */
public final class LogicalExecutor implements ConditionExecutor<Logical> {
    private static final Logger logger = LoggerFactory.getLogger(LogicalExecutor.class);
    
    private final ConditionExecutorFactory executorFactory;
    
    /**
     * Creates a new LogicalConditionExecutor that uses the provided factory to create
     * executors for subconditions.
     *
     * @param executorFactory The factory to use for creating condition executors
     */
    public LogicalExecutor(ConditionExecutorFactory executorFactory) {
        this.executorFactory = executorFactory;
    }
    
    @Override
    public Set<DocSentenceMatch> execute(Logical condition, Map<String, IndexAccess> indexes,
                               VariableBindings variableBindings, Query.Granularity granularity,
                               int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing logical condition with operator {} and {} subconditions at {} granularity with size {}",
                condition.operator(), condition.conditions().size(), granularity, granularitySize);
        
        List<Condition> subConditions = condition.conditions();
        if (subConditions.isEmpty()) {
            logger.debug("Logical condition has no subconditions, returning empty result set");
            return new HashSet<>();
        }
        
        // Process based on logical operator
        return switch (condition.operator()) {
            case AND -> executeAnd(subConditions, indexes, variableBindings, granularity, granularitySize);
            case OR -> executeOr(subConditions, indexes, variableBindings, granularity, granularitySize);
        };
    }
    
    private Set<DocSentenceMatch> executeAnd(
            List<Condition> conditions, 
            Map<String, IndexAccess> indexes,
            VariableBindings variableBindings,
            Query.Granularity granularity,
            int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing AND operation with {} conditions at {} granularity with size {}",
                conditions.size(), granularity, granularitySize);
        
        // Handle first condition
        Condition firstCondition = conditions.get(0);
        ConditionExecutor<Condition> executor = executorFactory.getExecutor(firstCondition);
        Set<DocSentenceMatch> result = executor.execute(firstCondition, indexes, variableBindings, granularity, granularitySize);
        
        if (result.isEmpty()) {
            // Short-circuit: if first condition has no results, AND will have no results
            logger.debug("First condition returned no results, short-circuiting AND");
            return result;
        }
        
        // Process remaining conditions
        for (int i = 1; i < conditions.size(); i++) {
            Condition nextCondition = conditions.get(i);
            executor = executorFactory.getExecutor(nextCondition);
            Set<DocSentenceMatch> nextResult = executor.execute(nextCondition, indexes, variableBindings, granularity, granularitySize);
            
            if (nextResult.isEmpty()) {
                // Short-circuit: if any condition has no results, AND will have no results
                logger.debug("Condition at index {} returned no results, short-circuiting AND", i);
                return new HashSet<>();
            }
            
            // Modified intersection handling for document granularity
            if (granularity == Query.Granularity.DOCUMENT) {
                // Group matches by document ID
                Map<Long, DocSentenceMatch> docMatches = result.stream()
                    .collect(Collectors.toMap(
                        m -> (long) m.documentId(),
                        m -> m,
                        (existing, replacement) -> {
                            existing.mergePositions(replacement);
                            return existing;
                        }
                    ));

                Set<Long> matchingDocs = nextResult.stream()
                    .map(m -> (long) m.documentId())
                    .collect(Collectors.toSet());

                // Retain only documents present in both sets
                docMatches.keySet().retainAll(matchingDocs);
                
                // Rebuild result with merged positions
                result = new HashSet<>(docMatches.values());
            } else {
                // Existing sentence-level intersection logic
                result = intersectMatches(result, nextResult, granularitySize);
            }
            
            if (result.isEmpty()) {
                // Short-circuit: if intersection is empty, AND will have no results
                logger.debug("Intersection at index {} is empty, short-circuiting AND", i);
                return result;
            }
        }
        
        logger.debug("AND operation completed with {} matching {}", 
                result.size(), granularity == Query.Granularity.DOCUMENT ? "documents" : "sentences");
        return result;
    }
    
    private Set<DocSentenceMatch> executeOr(
            List<Condition> conditions, 
            Map<String, IndexAccess> indexes,
            VariableBindings variableBindings,
            Query.Granularity granularity,
            int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing OR operation with {} conditions at {} granularity with size {}",
                conditions.size(), granularity, granularitySize);
        
        Set<DocSentenceMatch> result = new HashSet<>();
        
        // Process all conditions FIRST
        for (Condition subCondition : conditions) {
            ConditionExecutor<Condition> executor = executorFactory.getExecutor(subCondition);
            Set<DocSentenceMatch> subResult = executor.execute(subCondition, indexes, variableBindings, granularity, granularitySize);
            
            if (!subResult.isEmpty()) {
                if (result.isEmpty()) {
                    // First non-empty result, just add all
                    result.addAll(subResult);
                } else {
                    // Union with existing results
                    result = unionMatches(result, subResult);
                }
            }
        }
        
        // THEN handle document-level deduplication
        if (granularity == Query.Granularity.DOCUMENT) {
            Map<Long, DocSentenceMatch> merged = new HashMap<>();
            for (DocSentenceMatch match : result) {
                merged.merge((long) match.documentId(), match, (existing, newMatch) -> {
                    existing.mergePositions(newMatch);
                    return existing;
                });
            }
            return new HashSet<>(merged.values());
        }
        
        logger.debug("OR operation completed with {} matching {}", 
                result.size(), granularity == Query.Granularity.DOCUMENT ? "documents" : "sentences");
        return result;
    }
    
    /**
     * Combines two sets of matches using logical AND operation (intersection).
     * Preserves position information from both match sets.
     *
     * @param set1 First set of matches
     * @param set2 Second set of matches
     * @param windowSize Window size for sentence granularity (0 = same sentence only)
     * @return Intersection of the two sets with combined position information
     */
    public Set<DocSentenceMatch> intersectMatches(
            Set<DocSentenceMatch> set1,
            Set<DocSentenceMatch> set2,
            int windowSize) {

        if (set1.isEmpty() || set2.isEmpty()) {
            return new HashSet<>();
        }

        logger.debug("Intersecting matches with window size: {}", windowSize);

        Set<DocSentenceMatch> result = new HashSet<>();

        // Determine granularity from first match in set1
        boolean isSentenceLevel = !set1.isEmpty() && set1.iterator().next().isSentenceLevel();

        if (isSentenceLevel) {
            // Always use window-based matching, even for windowSize=0
            return intersectSentencesWithWindow(set1, set2, windowSize);
        } else {
            // Document-level intersection
            Map<Integer, DocSentenceMatch> documentMap = new HashMap<>();

            // Index first set by document ID
            for (DocSentenceMatch match : set1) {
                documentMap.put(match.documentId(), match);
            }

            // Check second set against the map
            for (DocSentenceMatch match2 : set2) {
                DocSentenceMatch match1 = documentMap.get(match2.documentId());

                if (match1 != null) {
                    // Create a new match with combined positions
                    DocSentenceMatch combined = new DocSentenceMatch(match1.documentId());

                    // Copy positions from both matches
                    copyPositions(match1, combined);
                    copyPositions(match2, combined);

                    result.add(combined);
                }
            }
        }

        return result;
    }
    
    /**
     * New version of sentence intersection that uses window-based matching
     */
    private Set<DocSentenceMatch> intersectSentencesWithWindow(
            Set<DocSentenceMatch> set1, 
            Set<DocSentenceMatch> set2,
            int windowSize) {
        
        logger.debug("Window-based intersection with window size: {}", windowSize);
        
        Set<DocSentenceMatch> result = new HashSet<>();
        
        // Special case for windowSize=0: use exact sentence matching
        if (windowSize == 0) {
            Map<SentenceKey, DocSentenceMatch> sentenceMap = new HashMap<>();
            
            // Index first set by sentence key
            for (DocSentenceMatch match : set1) {
                SentenceKey key = new SentenceKey(match.documentId(), match.sentenceId());
                sentenceMap.put(key, match);
            }
            
            // Check second set against the map
            for (DocSentenceMatch match2 : set2) {
                SentenceKey key = new SentenceKey(match2.documentId(), match2.sentenceId());
                DocSentenceMatch match1 = sentenceMap.get(key);
                
                if (match1 != null) {
                    // Create a new match with combined positions
                    DocSentenceMatch combined = new DocSentenceMatch(
                        match1.documentId(), match1.sentenceId());
                    
                    // Copy positions from both matches
                    copyPositions(match1, combined);
                    copyPositions(match2, combined);
                    
                    result.add(combined);
                }
            }
            
            return result;
        }
        
        // If windowSize > 0, use window-based matching
        
        // Group matches by document ID
        Map<Integer, List<DocSentenceMatch>> docToSentences1 = new HashMap<>();
        Map<Integer, List<DocSentenceMatch>> docToSentences2 = new HashMap<>();
        
        // Group set1 matches by document
        for (DocSentenceMatch match : set1) {
            int docId = match.documentId();
            docToSentences1.computeIfAbsent(docId, k -> new ArrayList<>()).add(match);
        }
        
        // Group set2 matches by document
        for (DocSentenceMatch match : set2) {
            int docId = match.documentId();
            docToSentences2.computeIfAbsent(docId, k -> new ArrayList<>()).add(match);
        }
        
        // Process each document that appears in both sets
        for (Integer docId : docToSentences1.keySet()) {
            List<DocSentenceMatch> sentences1 = docToSentences1.get(docId);
            List<DocSentenceMatch> sentences2 = docToSentences2.get(docId);
            
            if (sentences2 == null) {
                continue; // Document not in set2
            }
            
            // For each sentence in set1, find matches within the window in set2
            for (DocSentenceMatch match1 : sentences1) {
                int sentId1 = match1.sentenceId();
                
                for (DocSentenceMatch match2 : sentences2) {
                    int sentId2 = match2.sentenceId();
                    
                    // Check if sentences are within the window
                    if (Math.abs(sentId1 - sentId2) <= windowSize) {
                        // Match found within window - create one combined match
                        // We use the earlier sentence ID as the anchor
                        int anchorSentId = Math.min(sentId1, sentId2);
                        DocSentenceMatch combined = new DocSentenceMatch(docId, anchorSentId);
                        copyPositions(match1, combined);
                        copyPositions(match2, combined);
                        result.add(combined);
                    }
                }
            }
        }
        
        return result;
    }

    /**
     * Combines two sets of matches using logical OR operation (union).
     * Properly merges position information for overlapping matches.
     *
     * @param set1 First set of matches
     * @param set2 Second set of matches
     * @return Union of the two sets with merged position information
     */
    public Set<DocSentenceMatch> unionMatches(
            Set<DocSentenceMatch> set1,
            Set<DocSentenceMatch> set2) {

        if (set1.isEmpty()) {
            return new HashSet<>(set2);
        }

        if (set2.isEmpty()) {
            return new HashSet<>(set1);
        }

        Set<DocSentenceMatch> result = new HashSet<>();

        // Determine granularity from first match in set1
        boolean isSentenceLevel = set1.iterator().next().isSentenceLevel();

        if (isSentenceLevel) {
            // Sentence-level union
            Map<SentenceKey, DocSentenceMatch> sentenceMap = new HashMap<>();

            // Process first set
            for (DocSentenceMatch match : set1) {
                SentenceKey key = new SentenceKey(match.documentId(), match.sentenceId());
                sentenceMap.put(key, match);
            }

            // Process second set, merging with first set when overlapping
            for (DocSentenceMatch match2 : set2) {
                SentenceKey key = new SentenceKey(match2.documentId(), match2.sentenceId());
                DocSentenceMatch existing = sentenceMap.get(key);

                if (existing != null) {
                    // Merge positions into existing match
                    copyPositions(match2, existing);
                } else {
                    // Add new match to the map
                    sentenceMap.put(key, match2);
                }
            }

            // Convert map values to result set
            result.addAll(sentenceMap.values());
        } else {
            // Document-level union
            Map<Integer, DocSentenceMatch> documentMap = new HashMap<>();

            // Process first set
            for (DocSentenceMatch match : set1) {
                documentMap.put(match.documentId(), match);
            }

            // Process second set, merging with first set when overlapping
            for (DocSentenceMatch match2 : set2) {
                DocSentenceMatch existing = documentMap.get(match2.documentId());

                if (existing != null) {
                    // Merge positions into existing match
                    copyPositions(match2, existing);
                } else {
                    // Add new match to the map
                    documentMap.put(match2.documentId(), match2);
                }
            }

            // Convert map values to result set
            result.addAll(documentMap.values());
        }

        return result;
    }

    /**
     * Helper method to copy positions from one match to another
     */
    private void copyPositions(DocSentenceMatch from, DocSentenceMatch to) {
        // Copy all positions from source to target
        for (String key : from.getKeys()) {
            to.addPositions(key, from.getPositions(key));
        }
    }

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