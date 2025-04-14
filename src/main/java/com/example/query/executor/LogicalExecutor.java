package com.example.query.executor;

import com.example.core.IndexAccessInterface;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Logical.LogicalOperator;
import com.example.query.model.Query;
import com.example.query.binding.MatchDetail;
import java.util.stream.Stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for logical conditions (AND, OR).
 * Handles recursive execution and result combination of subconditions using QueryResult.
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
    public QueryResult execute(Logical condition, Map<String, IndexAccessInterface> indexes,
                               Query.Granularity granularity,
                               int granularitySize,
                               String corpusName)
        throws QueryExecutionException {
         QueryResult internalResult = executeInternal(condition, indexes, granularity, granularitySize, corpusName);
         return internalResult;
    }
    
    // --- Internal execution logic using QueryResult ---
    private QueryResult executeInternal(Logical condition, Map<String, IndexAccessInterface> indexes,
                                      Query.Granularity granularity,
                                      int granularitySize,
                                      String corpusName)
        throws QueryExecutionException {
        
        logger.debug("Executing logical condition internally: operator={}, subconditions={}, granularity={}, size={}, corpus={}",
                condition.operator(), condition.conditions().size(), granularity, granularitySize, corpusName);
        
        List<Condition> subConditions = condition.conditions();
        if (subConditions.isEmpty()) {
            logger.debug("Logical condition has no subconditions, returning empty QueryResult");
            return new QueryResult(granularity, granularitySize, Collections.emptyList());
        }
        
        LogicalOperator operator = condition.operator();
        if (operator == LogicalOperator.AND) {
            return executeAnd(subConditions, indexes, granularity, granularitySize, corpusName);
        } else if (operator == LogicalOperator.OR) {
            return executeOr(subConditions, indexes, granularity, granularitySize, corpusName);
        } else {
            throw new QueryExecutionException("Unsupported logical operator: " + operator, condition.toString(), QueryExecutionException.ErrorType.UNSUPPORTED_OPERATION);
        }
    }
    
    /**
     * Executes a logical AND, operating on QueryResult.
     */
    private QueryResult executeAnd(
            List<Condition> conditions, 
            Map<String, IndexAccessInterface> indexes,
            Query.Granularity granularity,
            int granularitySize,
            String corpusName)
        throws QueryExecutionException {
        
        logger.debug("Executing AND internally with {} subconditions (corpus: {})", conditions.size(), corpusName);

        QueryResult combinedResult = null;

        for (Condition condition : conditions) {
            ConditionExecutor executor = executorFactory.getExecutor(condition);
            QueryResult currentResult = executor.execute(condition, indexes, granularity, granularitySize, corpusName);

            if (currentResult.getAllDetails().isEmpty()) {
                logger.debug("Condition {} has no matches, short-circuiting AND", condition);
                return new QueryResult(granularity, granularitySize, Collections.emptyList());
            }
            
            if (combinedResult == null) {
                combinedResult = currentResult;
            } else {
                combinedResult = intersectQueryResults(combinedResult, currentResult);
                if (combinedResult.getAllDetails().isEmpty()) {
                    logger.debug("Intersection is empty, short-circuiting AND");
                    return combinedResult;
                }
            }
        }
        logger.debug("AND execution complete with {} details", combinedResult != null ? combinedResult.getAllDetails().size() : 0);
        return combinedResult != null ? combinedResult : new QueryResult(granularity, granularitySize, Collections.emptyList());
    }

    /**
     * Executes a logical OR, operating on QueryResult.
     */
    private QueryResult executeOr(
            List<Condition> conditions, 
            Map<String, IndexAccessInterface> indexes,
            Query.Granularity granularity,
            int granularitySize,
            String corpusName)
        throws QueryExecutionException {
        
        logger.debug("Executing OR internally with {} subconditions (corpus: {})", conditions.size(), corpusName);
        QueryResult combinedResult = null;
        
        for (Condition condition : conditions) {
            ConditionExecutor executor = executorFactory.getExecutor(condition);
            QueryResult currentResult = executor.execute(condition, indexes, granularity, granularitySize, corpusName);

            if (currentResult.getAllDetails().isEmpty()) {
                continue; // Skip empty results for OR
            }

            if (combinedResult == null) {
                combinedResult = currentResult;
            } else {
                combinedResult = unionQueryResults(combinedResult, currentResult);
            }
        }
        logger.debug("OR execution complete with {} details", combinedResult != null ? combinedResult.getAllDetails().size() : 0);
        return combinedResult != null ? combinedResult : new QueryResult(granularity, granularitySize, Collections.emptyList());
    }

    /**
     * Computes the union of two QueryResult objects.
     */
    private QueryResult unionQueryResults(QueryResult r1, QueryResult r2) {
        if (r1 == null) return r2;
        if (r2 == null) return r1;

        // Basic granularity check (can be enhanced)
        if (r1.getGranularity() != r2.getGranularity()) {
             logger.warn("Attempting to union QueryResults with different granularities: {} and {}. Using first result's granularity.",
                       r1.getGranularity(), r2.getGranularity());
             // Decide on a strategy: throw error, prefer finer, prefer first? Using first for now.
        }
        Query.Granularity resultGranularity = r1.getGranularity();
        int resultGranularitySize = r1.getGranularitySize();

        // Combine details - simple concatenation for now
        List<MatchDetail> combinedDetails = Stream.concat(
                r1.getAllDetails().stream(),
                r2.getAllDetails().stream()
        ).distinct() // Use distinct() based on MatchDetail record equality
         .collect(Collectors.toList());

        logger.debug("Union resulted in {} details", combinedDetails.size());
        return new QueryResult(resultGranularity, resultGranularitySize, combinedDetails);
    }

    // Define SentenceKey within LogicalExecutor or ensure it's accessible
    record SentenceKey(int documentId, int sentenceId) {}

    /**
     * Computes the intersection of two QueryResult objects.
     */
    private QueryResult intersectQueryResults(QueryResult r1, QueryResult r2) {
        // --- Start: Null/Empty/Granularity Checks (identical to QueryExecutor version) --- 
        if (r1 == null || r2 == null || r1.getAllDetails().isEmpty() || r2.getAllDetails().isEmpty()) {
             Query.Granularity defaultGranularity = (r1 != null) ? r1.getGranularity() : ((r2 != null) ? r2.getGranularity() : Query.Granularity.DOCUMENT);
             int defaultSize = (r1 != null) ? r1.getGranularitySize() : ((r2 != null) ? r2.getGranularitySize() : 0);
             return new QueryResult(defaultGranularity, defaultSize, Collections.emptyList());
        }
        Query.Granularity granularity = r1.getGranularity();
        if (r1.getGranularity() != r2.getGranularity() || r1.getGranularitySize() != r2.getGranularitySize()) {
            logger.error("Intersection of QueryResults with different granularities/sizes is not supported. ({},{}) vs ({},{})", r1.getGranularity(), r1.getGranularitySize(), r2.getGranularity(), r2.getGranularitySize());
            return new QueryResult(granularity, r1.getGranularitySize(), Collections.emptyList());
        }
        int windowSize = r1.getGranularitySize();
        // --- End: Null/Empty/Granularity Checks --- 

        if (granularity == Query.Granularity.DOCUMENT) {
            // --- Start: Document Granularity Logic (identical to QueryExecutor version) --- 
            Map<Integer, List<MatchDetail>> map1 = r1.getDetailsByDocId();
            Map<Integer, List<MatchDetail>> map2 = r2.getDetailsByDocId();
            Map<Integer, List<MatchDetail>> smallerMap = map1.size() < map2.size() ? map1 : map2;
            Map<Integer, List<MatchDetail>> largerMap = smallerMap == map1 ? map2 : map1;
            List<MatchDetail> combinedDocDetails = new ArrayList<>();
            for (Map.Entry<Integer, List<MatchDetail>> entry : smallerMap.entrySet()) {
                int docId = entry.getKey();
                if (largerMap.containsKey(docId)) {
                    List<MatchDetail> merged = new ArrayList<>(entry.getValue());
                    merged.addAll(largerMap.get(docId));
                    combinedDocDetails.addAll(merged);
                }
            }
            logger.trace("Intersection (DOCUMENT) resulted in {} details", combinedDocDetails.size());
            return new QueryResult(granularity, windowSize, combinedDocDetails);
            // --- End: Document Granularity Logic --- 

        } else { // Granularity.SENTENCE
            // --- Start: NEW Sentence Granularity Logic --- 
            Map<Integer, Map<Integer, List<MatchDetail>>> map1 = r1.getDetailsBySentence();
            Map<Integer, Map<Integer, List<MatchDetail>>> map2 = r2.getDetailsBySentence();
            
            Set<SentenceKey> matchingSentenceUnits = new HashSet<>();
            List<MatchDetail> combinedDetails = new ArrayList<>();

            // Find common document IDs
            Set<Integer> commonDocIds = new HashSet<>(map1.keySet());
            commonDocIds.retainAll(map2.keySet());

            // Calculate the allowed distance for the window
            int allowedDistance = (windowSize > 0) ? (windowSize - 1) / 2 : 0;

            for (int docId : commonDocIds) {
                Map<Integer, List<MatchDetail>> sentMap1 = map1.get(docId);
                Map<Integer, List<MatchDetail>> sentMap2 = map2.get(docId);
                Set<Integer> sentIds1 = sentMap1.keySet();
                Set<Integer> sentIds2 = sentMap2.keySet();
                
                 // <<< --- Add Debug Logging Here (Optional but helpful) --- >>>
                if (logger.isDebugEnabled()) { // Check if debug is enabled to avoid overhead
                    logger.debug("[intersectQueryResults] Doc {} - sentMap1 keys (r1): {}", docId, sentIds1);
                    logger.debug("[intersectQueryResults] Doc {} - sentMap2 keys (r2): {}", docId, sentIds2);
                }
                // <<< --- End Debug Logging --- >>>

                // Check from map1's perspective
                for (int sentId1 : sentIds1) {
                    boolean foundMatchInWindow = false;
                    for (int sentId2 : sentIds2) {
                        if (Math.abs(sentId1 - sentId2) <= allowedDistance) {
                            foundMatchInWindow = true;
                            break;
                        }
                    }
                    if (foundMatchInWindow) {
                        matchingSentenceUnits.add(new SentenceKey(docId, sentId1));
                    }
                }

                // Check from map2's perspective
                for (int sentId2 : sentIds2) {
                    boolean foundMatchInWindow = false;
                    for (int sentId1 : sentIds1) {
                        if (Math.abs(sentId1 - sentId2) <= allowedDistance) {
                            foundMatchInWindow = true;
                            break;
                        }
                    }
                    if (foundMatchInWindow) {
                        matchingSentenceUnits.add(new SentenceKey(docId, sentId2));
                    }
                }
            } // End loop through commonDocIds

            // Collect all details for the identified matching sentence units
            for (SentenceKey unit : matchingSentenceUnits) {
                 int docId = unit.documentId();
                 int sentId = unit.sentenceId();
                 
                 // Add details from r1 for this unit
                 List<MatchDetail> details1 = map1.getOrDefault(docId, Collections.emptyMap()).get(sentId);
                 if (details1 != null) {
                      combinedDetails.addAll(details1);
                 }
                 
                 // Add details from r2 for this unit
                 List<MatchDetail> details2 = map2.getOrDefault(docId, Collections.emptyMap()).get(sentId);
                 if (details2 != null) {
                      combinedDetails.addAll(details2);
                 }
            }
            
            // Remove potential duplicates if a detail was present in both r1 and r2 for the same sentence
            List<MatchDetail> finalUniqueDetails = new ArrayList<>(new HashSet<>(combinedDetails));

            logger.trace("Intersection (SENTENCE, window={}, distance={}) resulted in {} final details from {} matching units", 
                     windowSize, allowedDistance, finalUniqueDetails.size(), matchingSentenceUnits.size());
            return new QueryResult(granularity, windowSize, finalUniqueDetails);
            // --- End: NEW Sentence Granularity Logic --- 
        }
    }
} 