package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Logical.LogicalOperator;
import com.example.query.model.Query;
import com.example.query.binding.BindingContext;

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
                               BindingContext bindingContext, Query.Granularity granularity,
                               int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing logical condition with operator {} and {} subconditions at {} granularity with size {}",
                condition.operator(), condition.conditions().size(), granularity, granularitySize);
        
        List<Condition> subConditions = condition.conditions();
        if (subConditions.isEmpty()) {
            logger.debug("Logical condition has no subconditions, returning empty result set");
            return new HashSet<>();
        }
        
        LogicalOperator operator = condition.operator();
        if (operator == LogicalOperator.AND) {
            return executeAnd(subConditions, indexes, bindingContext, granularity, granularitySize);
        } else if (operator == LogicalOperator.OR) {
            return executeOr(subConditions, indexes, bindingContext, granularity, granularitySize);
        } else {
            throw new QueryExecutionException(
                "Unsupported logical operator: " + operator,
                condition.toString(),
                QueryExecutionException.ErrorType.UNSUPPORTED_OPERATION
            );
        }
    }
    
    /**
     * Executes a logical AND condition.
     * All subconditions must match for a result to be included.
     */
    private Set<DocSentenceMatch> executeAnd(
            List<Condition> conditions, 
            Map<String, IndexAccess> indexes,
            BindingContext bindingContext,
            Query.Granularity granularity,
            int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing AND condition with {} subconditions", conditions.size());
        
        // Optimize execution order based on variable dependencies
        List<Condition> orderedConditions = optimizeExecutionOrder(conditions);
        
        // Base case: single condition
        if (orderedConditions.size() == 1) {
            Condition condition = orderedConditions.get(0);
            ConditionExecutor executor = executorFactory.getExecutor(condition);
            return executor.execute(condition, indexes, bindingContext, granularity, granularitySize);
        }
        
        // Copy binding context to avoid modifying the original
        BindingContext workingContext = bindingContext.copy();
        Set<DocSentenceMatch> result = null;
        
        // Execute each condition in sequence, intersecting results
        for (Condition condition : orderedConditions) {
            // Prepare binding context for this condition
            workingContext = condition.prepareBindingContext(workingContext);
            
            // Get appropriate executor for this condition
            ConditionExecutor executor = executorFactory.getExecutor(condition);
            
            // Execute the condition
            Set<DocSentenceMatch> conditionMatches = executor.execute(
                condition, indexes, workingContext, granularity, granularitySize);
            
            if (conditionMatches.isEmpty()) {
                // Short-circuit: if any condition has no matches, AND will have no matches
                logger.debug("Condition {} has no matches, short-circuiting AND", condition);
                return new HashSet<>();
            }
            
            if (result == null) {
                // First condition, just use its results
                result = conditionMatches;
            } else {
                // Intersect with previous results
                if (granularity == Query.Granularity.SENTENCE && granularitySize > 0) {
                    // For sentence granularity with window, use windowed intersection
                    result = intersectSentencesWithWindow(result, conditionMatches, granularitySize);
                } else {
                    // For document granularity or sentence without window, use simple intersection
                    result = intersectMatches(result, conditionMatches, granularitySize);
                }
                
                if (result.isEmpty()) {
                    // Short-circuit: if intersection is empty, AND will have no matches
                    logger.debug("Intersection is empty, short-circuiting AND");
                    return new HashSet<>();
                }
            }
        }
        
        // Merge the final working context back to the original
        bindingContext = workingContext;
        
        logger.debug("AND execution complete with {} matches", result != null ? result.size() : 0);
        
        return result != null ? result : new HashSet<>();
    }
    
    /**
     * Executes a logical OR condition.
     * At least one subcondition must match for a result to be included.
     */
    private Set<DocSentenceMatch> executeOr(
            List<Condition> conditions, 
            Map<String, IndexAccess> indexes,
            BindingContext bindingContext,
            Query.Granularity granularity,
            int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing OR condition with {} subconditions", conditions.size());
        
        // Base case: single condition
        if (conditions.size() == 1) {
            Condition condition = conditions.get(0);
            ConditionExecutor executor = executorFactory.getExecutor(condition);
            return executor.execute(condition, indexes, bindingContext, granularity, granularitySize);
        }
        
        // For OR, we need to execute all conditions and union their results
        Set<DocSentenceMatch> result = new HashSet<>();
        
        // Copy binding context to avoid modifying the original during execution
        BindingContext combinedContext = bindingContext.copy();
        
        for (Condition condition : conditions) {
            // Create a copy of the binding context for this condition
            BindingContext conditionContext = bindingContext.copy();
            
            // Get appropriate executor for this condition
            ConditionExecutor executor = executorFactory.getExecutor(condition);
            
            // Execute the condition
            Set<DocSentenceMatch> conditionMatches = executor.execute(
                condition, indexes, conditionContext, granularity, granularitySize);
            
            if (!conditionMatches.isEmpty()) {
                // Union with previous results
                result = unionMatches(result, conditionMatches);
                
                // Merge this condition's context into the combined context
                combinedContext = combinedContext.merge(conditionContext);
            }
        }
        
        // Update the original binding context with all bindings from all conditions
        bindingContext = combinedContext;
        
        logger.debug("OR execution complete with {} matches", result.size());
        
        return result;
    }
    
    /**
     * Optimizes the execution order of conditions based on variable dependencies.
     * Conditions that produce variables should be executed before conditions that consume them.
     *
     * @param conditions The original list of conditions
     * @return A new list with optimized execution order
     */
    private List<Condition> optimizeExecutionOrder(List<Condition> conditions) {
        if (conditions.size() <= 1) {
            return conditions;
        }
        
        // Create a copy to manipulate
        List<Condition> remaining = new ArrayList<>(conditions);
        List<Condition> ordered = new ArrayList<>();
        Set<String> availableVariables = new HashSet<>();
        
        // Continue until all conditions are ordered
        while (!remaining.isEmpty()) {
            boolean progress = false;
            
            // Find conditions that can be executed with available variables
            for (int i = 0; i < remaining.size(); i++) {
                Condition condition = remaining.get(i);
                
                // Check if all consumed variables are available
                boolean canExecute = true;
                for (String var : condition.getConsumedVariables()) {
                    if (!availableVariables.contains(var)) {
                        canExecute = false;
                        break;
                    }
                }
                
                if (canExecute) {
                    // Add condition to ordered list
                    ordered.add(condition);
                    remaining.remove(i);
                    
                    // Add produced variables to available set
                    availableVariables.addAll(condition.getProducedVariables());
                    
                    progress = true;
                    break;
                }
            }
            
            // If no progress was made, we have a circular dependency
            // In this case, add the next condition and continue
            if (!progress && !remaining.isEmpty()) {
                Condition next = remaining.remove(0);
                ordered.add(next);
                availableVariables.addAll(next.getProducedVariables());
            }
        }
        
        logger.debug("Optimized execution order: {}", ordered.stream().map(Condition::getType).collect(Collectors.toList()));
        return ordered;
    }
    
    /**
     * Intersects two sets of matches.
     * For document granularity, matches are considered equal if they have the same document ID.
     * For sentence granularity, matches are considered equal if they have the same document ID and sentence ID.
     *
     * @param set1 First set of matches
     * @param set2 Second set of matches
     * @param windowSize Window size for sentence matching (0 for exact, > 0 for window)
     * @return Intersection of the two sets
     */
    public Set<DocSentenceMatch> intersectMatches(
            Set<DocSentenceMatch> set1,
            Set<DocSentenceMatch> set2,
            int windowSize) {
        
        if (set1.isEmpty() || set2.isEmpty()) {
            return new HashSet<>();
        }
        
        // Determine if we're working with document or sentence granularity
        boolean isSentenceGranularity = set1.iterator().next().isSentenceLevel();
        
        if (isSentenceGranularity && windowSize > 0) {
            // Sentence granularity with window, use special intersection
            return intersectSentencesWithWindow(set1, set2, windowSize);
        }
        
        Set<DocSentenceMatch> result = new HashSet<>();
        
        if (isSentenceGranularity) {
            // For sentence granularity without window, index by document ID and sentence ID
            Map<SentenceKey, DocSentenceMatch> set1Map = new HashMap<>();
            for (DocSentenceMatch match : set1) {
                SentenceKey key = new SentenceKey(match.documentId(), match.sentenceId());
                set1Map.put(key, match);
            }
            
            // Iterate through set2 and find matches in set1
            for (DocSentenceMatch match2 : set2) {
                SentenceKey key = new SentenceKey(match2.documentId(), match2.sentenceId());
                DocSentenceMatch match1 = set1Map.get(key);
                
                if (match1 != null) {
                    // Combine positions from both matches
                    DocSentenceMatch combined = new DocSentenceMatch(
                        match1.documentId(), 
                        match1.sentenceId(), 
                        match1.getSource()
                    );
                    
                    // Copy positions from both matches
                    copyPositions(match1, combined);
                    copyPositions(match2, combined);
                    
                    result.add(combined);
                }
            }
        } else {
            // For document granularity, index by document ID
            Map<Integer, DocSentenceMatch> set1Map = new HashMap<>();
            for (DocSentenceMatch match : set1) {
                set1Map.put(match.documentId(), match);
            }
            
            // Iterate through set2 and find matches in set1
            for (DocSentenceMatch match2 : set2) {
                DocSentenceMatch match1 = set1Map.get(match2.documentId());
                
                if (match1 != null) {
                    // Combine positions from both matches
                    DocSentenceMatch combined = new DocSentenceMatch(
                        match1.documentId(), 
                        match1.getSource()
                    );
                    
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
     * Intersects two sets of sentence-level matches with a window.
     * Sentences are considered to match if they are within windowSize of each other.
     *
     * @param set1 First set of matches
     * @param set2 Second set of matches
     * @param windowSize Window size for sentence matching
     * @return Intersection of the two sets
     */
    private Set<DocSentenceMatch> intersectSentencesWithWindow(
            Set<DocSentenceMatch> set1, 
            Set<DocSentenceMatch> set2,
            int windowSize) {
        
        if (set1.isEmpty() || set2.isEmpty()) {
            return new HashSet<>();
        }
        
        if (windowSize <= 0) {
            // For window size 0, use normal intersection
            return intersectMatches(set1, set2, 0);
        }
        
        Set<DocSentenceMatch> result = new HashSet<>();
        
        // Group matches by document ID
        Map<Integer, List<DocSentenceMatch>> set1ByDoc = set1.stream()
            .collect(Collectors.groupingBy(DocSentenceMatch::documentId));
        
        Map<Integer, List<DocSentenceMatch>> set2ByDoc = set2.stream()
            .collect(Collectors.groupingBy(DocSentenceMatch::documentId));
        
        // Find documents in both sets
        Set<Integer> docsInBoth = new HashSet<>(set1ByDoc.keySet());
        docsInBoth.retainAll(set2ByDoc.keySet());
        
        // For each document in both sets, find sentences within window
        for (Integer docId : docsInBoth) {
            List<DocSentenceMatch> doc1Matches = set1ByDoc.get(docId);
            List<DocSentenceMatch> doc2Matches = set2ByDoc.get(docId);
            
            // For each sentence in doc1, find nearby sentences in doc2
            for (DocSentenceMatch match1 : doc1Matches) {
                int sentId1 = match1.sentenceId();
                
                for (DocSentenceMatch match2 : doc2Matches) {
                    int sentId2 = match2.sentenceId();
                    
                    // Check if sentences are within window
                    if (Math.abs(sentId1 - sentId2) <= windowSize) {
                        // For each pair within window, create a match at the later sentence
                        int resultSentId = Math.max(sentId1, sentId2);
                        
                        DocSentenceMatch combined = new DocSentenceMatch(
                            docId, resultSentId, match1.getSource()
                        );
                        
                        // Copy positions from both matches
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
     * Unions two sets of matches, combining positions for matches with the same document/sentence ID.
     *
     * @param set1 First set of matches
     * @param set2 Second set of matches
     * @return Union of the two sets
     */
    public Set<DocSentenceMatch> unionMatches(
            Set<DocSentenceMatch> set1,
            Set<DocSentenceMatch> set2) {
        
        if (set1.isEmpty()) {
            return set2;
        }
        
        if (set2.isEmpty()) {
            return set1;
        }
        
        // Determine if we're working with document or sentence granularity
        boolean isSentenceGranularity = set1.iterator().next().isSentenceLevel();
        
        Set<DocSentenceMatch> result = new HashSet<>();
        
        if (isSentenceGranularity) {
            // For sentence granularity, index by document ID and sentence ID
            Map<SentenceKey, DocSentenceMatch> resultMap = new HashMap<>();
            
            // Add matches from set1
            for (DocSentenceMatch match : set1) {
                SentenceKey key = new SentenceKey(match.documentId(), match.sentenceId());
                resultMap.put(key, match);
            }
            
            // Add matches from set2, combining positions if needed
            for (DocSentenceMatch match : set2) {
                SentenceKey key = new SentenceKey(match.documentId(), match.sentenceId());
                DocSentenceMatch existing = resultMap.get(key);
                
                if (existing != null) {
                    // Combine positions from both matches
                    DocSentenceMatch combined = new DocSentenceMatch(
                        existing.documentId(), 
                        existing.sentenceId(), 
                        existing.getSource()
                    );
                    
                    // Copy positions from both matches
                    copyPositions(existing, combined);
                    copyPositions(match, combined);
                    
                    resultMap.put(key, combined);
                } else {
                    // No existing match, just add this one
                    resultMap.put(key, match);
                }
            }
            
            result.addAll(resultMap.values());
        } else {
            // For document granularity, index by document ID
            Map<Integer, DocSentenceMatch> resultMap = new HashMap<>();
            
            // Add matches from set1
            for (DocSentenceMatch match : set1) {
                resultMap.put(match.documentId(), match);
            }
            
            // Add matches from set2, combining positions if needed
            for (DocSentenceMatch match : set2) {
                DocSentenceMatch existing = resultMap.get(match.documentId());
                
                if (existing != null) {
                    // Combine positions from both matches
                    DocSentenceMatch combined = new DocSentenceMatch(
                        existing.documentId(), 
                        existing.getSource()
                    );
                    
                    // Copy positions from both matches
                    copyPositions(existing, combined);
                    copyPositions(match, combined);
                    
                    resultMap.put(match.documentId(), combined);
                } else {
                    // No existing match, just add this one
                    resultMap.put(match.documentId(), match);
                }
            }
            
            result.addAll(resultMap.values());
        }
        
        return result;
    }
    
    /**
     * Copies all positions from one match to another.
     * Also copies variable values.
     */
    private void copyPositions(DocSentenceMatch from, DocSentenceMatch to) {
        from.getAllPositions().forEach(to::addPositions);
        
        // Copy variable values from 'from' to 'to'
        from.getVariableValues().forEach(to::setVariableValue);
    }
    
    /**
     * Helper record for sentence identification.
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