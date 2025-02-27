package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.query.model.Condition;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.LogicalCondition;
import com.example.query.model.LogicalCondition.LogicalOperator;
import com.example.query.model.Query;

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
 * Executor for LogicalCondition.
 * Handles AND, OR, and NOT operations between conditions.
 */
public class LogicalConditionExecutor implements ConditionExecutor<LogicalCondition> {
    private static final Logger logger = LoggerFactory.getLogger(LogicalConditionExecutor.class);
    
    private final ConditionExecutorFactory executorFactory;
    
    /**
     * Creates a new LogicalConditionExecutor that uses the provided factory to create
     * executors for subconditions.
     *
     * @param executorFactory The factory to use for creating condition executors
     */
    public LogicalConditionExecutor(ConditionExecutorFactory executorFactory) {
        this.executorFactory = executorFactory;
    }
    
    @Override
    public Set<DocSentenceMatch> execute(LogicalCondition condition, Map<String, IndexAccess> indexes,
                               VariableBindings variableBindings, Query.Granularity granularity)
        throws QueryExecutionException {
        
        LogicalOperator operator = condition.getOperator();
        List<Condition> conditions = condition.getConditions();
        
        logger.debug("Executing {} operation with {} conditions at {} granularity", 
                operator, conditions.size(), granularity);
        
        if (conditions.isEmpty()) {
            // This should never happen as the constructor validates this
            logger.warn("Logical condition has no subconditions");
            return new HashSet<>();
        }
        
        try {
            switch (operator) {
                case AND:
                    return executeAnd(conditions, indexes, variableBindings, granularity);
                case OR:
                    return executeOr(conditions, indexes, variableBindings, granularity);
                default:
                    throw new QueryExecutionException(
                        "Unsupported logical operator: " + operator,
                        condition.toString(),
                        QueryExecutionException.ErrorType.UNSUPPORTED_OPERATION
                    );
            }
        } catch (Exception e) {
            if (e instanceof QueryExecutionException) {
                throw (QueryExecutionException) e;
            }
            throw new QueryExecutionException(
                "Error executing logical condition: " + e.getMessage(),
                e,
                condition.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Executes an AND operation between multiple conditions.
     * Returns the intersection of all subcondition results.
     *
     * @param conditions The conditions to AND together
     * @param indexes The indexes to use
     * @param variableBindings The variable bindings to update
     * @param granularity The query granularity
     * @return The set of matches matching all conditions
     */
    private Set<DocSentenceMatch> executeAnd(
            List<Condition> conditions, 
            Map<String, IndexAccess> indexes,
            VariableBindings variableBindings,
            Query.Granularity granularity)
        throws QueryExecutionException {
        
        logger.debug("Executing AND operation with {} conditions at {} granularity", 
                conditions.size(), granularity);
        
        // Handle first condition
        Condition firstCondition = conditions.get(0);
        ConditionExecutor<Condition> executor = executorFactory.getExecutor(firstCondition);
        Set<DocSentenceMatch> result = executor.execute(firstCondition, indexes, variableBindings, granularity);
        
        if (result.isEmpty()) {
            // Short-circuit: if first condition has no results, AND will have no results
            logger.debug("First condition returned no results, short-circuiting AND");
            return result;
        }
        
        // Process remaining conditions
        for (int i = 1; i < conditions.size(); i++) {
            Condition nextCondition = conditions.get(i);
            executor = executorFactory.getExecutor(nextCondition);
            Set<DocSentenceMatch> nextResult = executor.execute(nextCondition, indexes, variableBindings, granularity);
            
            if (nextResult.isEmpty()) {
                // Short-circuit: if any condition has no results, AND will have no results
                logger.debug("Condition {} returned no results, short-circuiting AND", i);
                return new HashSet<>();
            }
            
            // Intersection based on granularity and match properties
            result = intersectMatches(result, nextResult);
            
            if (result.isEmpty()) {
                // Short-circuit: if intersection is empty, AND will have no results
                logger.debug("AND operation resulted in empty set after condition {}, short-circuiting", i);
                return result;
            }
        }
        
        logger.debug("AND operation completed with {} matching {}", 
                result.size(), granularity == Query.Granularity.DOCUMENT ? "documents" : "sentences");
        return result;
    }
    
    /**
     * Executes an OR operation between multiple conditions.
     * Returns the union of all subcondition results.
     *
     * @param conditions The conditions to OR together
     * @param indexes The indexes to use
     * @param variableBindings The variable bindings to update
     * @param granularity The query granularity
     * @return The set of matches matching any condition
     */
    private Set<DocSentenceMatch> executeOr(
            List<Condition> conditions, 
            Map<String, IndexAccess> indexes,
            VariableBindings variableBindings,
            Query.Granularity granularity)
        throws QueryExecutionException {
        
        logger.debug("Executing OR operation with {} conditions at {} granularity", 
                conditions.size(), granularity);
        
        Set<DocSentenceMatch> result = new HashSet<>();
        
        // Process all conditions and union the results
        for (Condition subCondition : conditions) {
            ConditionExecutor<Condition> executor = executorFactory.getExecutor(subCondition);
            Set<DocSentenceMatch> subResult = executor.execute(subCondition, indexes, variableBindings, granularity);
            
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
     * @return Intersection of the two sets with combined position information
     */
    public Set<DocSentenceMatch> intersectMatches(
            Set<DocSentenceMatch> set1,
            Set<DocSentenceMatch> set2) {

        if (set1.isEmpty() || set2.isEmpty()) {
            return new HashSet<>();
        }

        Set<DocSentenceMatch> result = new HashSet<>();

        // Determine granularity from first match in set1
        boolean isSentenceLevel = !set1.isEmpty() && set1.iterator().next().isSentenceLevel();

        if (isSentenceLevel) {
            // Sentence-level intersection
            Map<SentenceKey, DocSentenceMatch> sentenceMap = new HashMap<>();

            // Index first set by sentence key
            for (DocSentenceMatch match : set1) {
                SentenceKey key = new SentenceKey(match.getDocumentId(), match.getSentenceId());
                sentenceMap.put(key, match);
            }

            // Check second set against the map
            for (DocSentenceMatch match2 : set2) {
                SentenceKey key = new SentenceKey(match2.getDocumentId(), match2.getSentenceId());
                DocSentenceMatch match1 = sentenceMap.get(key);

                if (match1 != null) {
                    // Create a new match with combined positions
                    DocSentenceMatch combined = new DocSentenceMatch(
                        match1.getDocumentId(), match1.getSentenceId());

                    // Copy positions from both matches
                    copyPositions(match1, combined);
                    copyPositions(match2, combined);

                    result.add(combined);
                }
            }
        } else {
            // Document-level intersection
            Map<Integer, DocSentenceMatch> documentMap = new HashMap<>();

            // Index first set by document ID
            for (DocSentenceMatch match : set1) {
                documentMap.put(match.getDocumentId(), match);
            }

            // Check second set against the map
            for (DocSentenceMatch match2 : set2) {
                DocSentenceMatch match1 = documentMap.get(match2.getDocumentId());

                if (match1 != null) {
                    // Create a new match with combined positions
                    DocSentenceMatch combined = new DocSentenceMatch(match1.getDocumentId());

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
                SentenceKey key = new SentenceKey(match.getDocumentId(), match.getSentenceId());
                sentenceMap.put(key, match);
            }

            // Process second set, merging with first set when overlapping
            for (DocSentenceMatch match2 : set2) {
                SentenceKey key = new SentenceKey(match2.getDocumentId(), match2.getSentenceId());
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
                documentMap.put(match.getDocumentId(), match);
            }

            // Process second set, merging with first set when overlapping
            for (DocSentenceMatch match2 : set2) {
                DocSentenceMatch existing = documentMap.get(match2.getDocumentId());

                if (existing != null) {
                    // Merge positions into existing match
                    copyPositions(match2, existing);
                } else {
                    // Add new match to the map
                    documentMap.put(match2.getDocumentId(), match2);
                }
            }

            // Convert map values to result set
            result.addAll(documentMap.values());
        }

        return result;
    }

    /**
     * Helper class for sentence identification
     */
    private static class SentenceKey {
        private final int documentId;
        private final int sentenceId;

        public SentenceKey(int documentId, int sentenceId) {
            this.documentId = documentId;
            this.sentenceId = sentenceId;
        }

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

    /**
     * Helper method to copy positions from one match to another
     */
    private void copyPositions(DocSentenceMatch source, DocSentenceMatch target) {
        for (String key : source.getKeys()) {
            for (Position position : source.getPositions(key)) {
                target.addPosition(key, position);
            }
        }
    }
} 