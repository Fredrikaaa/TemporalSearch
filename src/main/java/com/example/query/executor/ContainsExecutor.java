package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Contains;
import com.example.query.binding.BindingContext;

import java.util.Objects;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for CONTAINS conditions.
 * Handles n-gram pattern matching and variable binding.
 * 
 * @see com.example.query.model.condition.Contains
 */
public final class ContainsExecutor implements ConditionExecutor<Contains> {
    private static final Logger logger = LoggerFactory.getLogger(ContainsExecutor.class);
    
    private static final String UNIGRAM_INDEX = "unigram";
    private static final String BIGRAM_INDEX = "bigram";
    private static final String TRIGRAM_INDEX = "trigram";
    private static final char DELIMITER = IndexAccess.NGRAM_DELIMITER;

    /**
     * Creates a new ContainsExecutor.
     */
    public ContainsExecutor() {
        // No initialization required
    }

    @Override
    public Set<DocSentenceMatch> execute(Contains condition, Map<String, IndexAccess> indexes,
                               BindingContext bindingContext, Query.Granularity granularity,
                               int granularitySize) 
        throws QueryExecutionException {
        
        logger.debug("Executing CONTAINS condition with {} terms at {} granularity with size {}", 
                condition.terms().size(), granularity, granularitySize);
        
        // Validate required indexes
        if (!indexes.containsKey(UNIGRAM_INDEX)) {
            throw new QueryExecutionException(
                "Missing required unigram index",
                condition.toString(),
                QueryExecutionException.ErrorType.MISSING_INDEX
            );
        }
        
        Set<DocSentenceMatch> matches = new HashSet<>();
        
        List<String> terms = condition.terms();
        if (terms.isEmpty()) {
            logger.warn("CONTAINS condition has no terms, returning empty result set");
            return matches;
        }
        
        // Check if there are too many terms
        if (terms.size() > 3) {
            throw new QueryExecutionException(
                "CONTAINS condition supports at most 3 terms, but got " + terms.size() + " terms",
                "CONTAINS(" + String.join(", ", terms) + ")",
                QueryExecutionException.ErrorType.INVALID_CONDITION
            );
        }
        
        // Determine if this is a variable binding
        boolean isVariable = condition.isVariable();
        String variableName = condition.variableName();
        
        // Get the appropriate ngram index based on the size of the terms
        IndexAccess index = null;
        if (terms.size() == 1) {
            index = indexes.get(UNIGRAM_INDEX);
        } else if (terms.size() == 2) {
            index = indexes.get(BIGRAM_INDEX);
        } else if (terms.size() == 3) {
            index = indexes.get(TRIGRAM_INDEX);
        }
        
        if (index == null) {
            throw new QueryExecutionException(
                "No index found for " + terms.size() + "-gram terms. Available indexes: " + indexes.keySet(),
                "CONTAINS(" + String.join(", ", terms) + ")",
                QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR
            );
        }
        
        try {
            // Construct search patterns based on the terms
            Set<String> patterns = constructSearchPatterns(terms);
            
            // Execute search for each pattern and union the results
            for (String pattern : patterns) {
                Set<DocSentenceMatch> patternMatches = executePatternSearch(
                    pattern, isVariable, variableName, index, bindingContext, granularity);
                
                if (!patternMatches.isEmpty()) {
                    matches.addAll(patternMatches);
                }
            }
            
            logger.debug("Found {} matches for terms: {}", matches.size(), terms);
            return matches;
        } catch (Exception e) {
            throw new QueryExecutionException(
                "Error executing CONTAINS condition: " + e.getMessage(),
                e,
                condition.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Constructs search patterns from terms, handling wildcards.
     * For example, ["apple", "*", "day"] would generate patterns for all trigrams
     * starting with "apple" and ending with "day".
     *
     * @param terms The list of terms, possibly containing wildcards
     * @return Set of search patterns to look for
     */
    private Set<String> constructSearchPatterns(List<String> terms) {
        Set<String> patterns = new HashSet<>();
        
        // Check if there are any wildcards
        boolean hasWildcard = terms.stream().anyMatch(term -> "*".equals(term));
        
        if (!hasWildcard) {
            // No wildcards, join the terms with the appropriate delimiter based on term count
            if (terms.size() == 1) {
                // For unigrams, just use the term itself
                patterns.add(terms.get(0).toLowerCase());
            } else if (terms.size() == 2) {
                // For bigrams, use null byte delimiter
                patterns.add(terms.get(0).toLowerCase() + DELIMITER + terms.get(1).toLowerCase());
            } else if (terms.size() == 3) {
                // For trigrams, use null byte delimiter
                patterns.add(terms.get(0).toLowerCase() + DELIMITER + terms.get(1).toLowerCase() + DELIMITER + terms.get(2).toLowerCase());
            }
            return patterns;
        }
        
        // Handle wildcards based on the number of terms
        if (terms.size() == 2) {
            // Bigram with one wildcard
            if ("*".equals(terms.get(0))) {
                // Wildcard in first position - we'd need to scan all bigrams ending with the second term
                // This is not efficient, so we'll log a warning
                logger.warn("Wildcard in first position of bigram is not efficiently supported: {}", terms);
                patterns.add("*" + DELIMITER + terms.get(1).toLowerCase());
            } else if ("*".equals(terms.get(1))) {
                // Wildcard in second position - we'd need to scan all bigrams starting with the first term
                // This is not efficient, so we'll log a warning
                logger.warn("Wildcard in second position of bigram is not efficiently supported: {}", terms);
                patterns.add(terms.get(0).toLowerCase() + DELIMITER + "*");
            }
        } else if (terms.size() == 3) {
            // Trigram with wildcards
            if ("*".equals(terms.get(1))) {
                // Middle term is wildcard - we'd need to scan for all trigrams with first and last terms
                // This is not efficient, so we'll log a warning
                logger.warn("Wildcard in middle position of trigram is not efficiently supported: {}", terms);
                patterns.add(terms.get(0).toLowerCase() + DELIMITER + "*" + DELIMITER + terms.get(2).toLowerCase());
            } else if ("*".equals(terms.get(0))) {
                // First term is wildcard
                logger.warn("Wildcard in first position of trigram is not efficiently supported: {}", terms);
                patterns.add("*" + DELIMITER + terms.get(1).toLowerCase() + DELIMITER + terms.get(2).toLowerCase());
            } else if ("*".equals(terms.get(2))) {
                // Last term is wildcard
                logger.warn("Wildcard in last position of trigram is not efficiently supported: {}", terms);
                patterns.add(terms.get(0).toLowerCase() + DELIMITER + terms.get(1).toLowerCase() + DELIMITER + "*");
            }
        }
        
        // If we couldn't create any patterns (shouldn't happen), use the original terms with appropriate delimiter
        if (patterns.isEmpty()) {
            if (terms.size() == 1) {
                patterns.add(terms.get(0).toLowerCase());
            } else if (terms.size() == 2) {
                patterns.add(terms.get(0).toLowerCase() + DELIMITER + terms.get(1).toLowerCase());
            } else if (terms.size() == 3) {
                patterns.add(terms.get(0).toLowerCase() + DELIMITER + terms.get(1).toLowerCase() + DELIMITER + terms.get(2).toLowerCase());
            }
        }
        
        return patterns;
    }
    
    /**
     * Executes a search for a specific pattern.
     *
     * @param pattern The pattern to search for
     * @param isVariable Whether this is a variable binding
     * @param variableName The variable name (if isVariable is true)
     * @param index The index to search in
     * @param bindingContext The binding context to update
     * @param granularity The granularity of the search
     * @return Set of matches at the specified granularity level
     */
    private Set<DocSentenceMatch> executePatternSearch(String pattern, boolean isVariable, String variableName,
                                        IndexAccess index, BindingContext bindingContext, 
                                        Query.Granularity granularity)
        throws QueryExecutionException {
        
        // Skip empty patterns
        if (pattern == null || pattern.trim().isEmpty()) {
            logger.warn("Skipping empty pattern in CONTAINS condition");
            return new HashSet<>();
        }
        
        // Check if pattern contains wildcards
        if (pattern.contains("*")) {
            // For now, we'll just log a warning and return empty results
            // In a full implementation, we'd need to scan the index for matching patterns
            logger.warn("Wildcard patterns are not fully implemented yet: {}", pattern);
            return new HashSet<>();
        }
        
        // Normalize pattern to lowercase
        String normalizedPattern = pattern.toLowerCase();
        logger.info("Searching for pattern '{}' in index type {}", normalizedPattern, index.getIndexType());
        
        try {
            // Convert to bytes
            byte[] patternBytes = normalizedPattern.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            logger.info("Pattern as bytes: {}", java.util.Arrays.toString(patternBytes));
            
            // Search the index
            Optional<PositionList> positionsOpt = index.get(patternBytes);
                
            if (!positionsOpt.isPresent()) {
                // Pattern not found in any documents
                logger.info("Pattern '{}' not found in any documents", normalizedPattern);
                return new HashSet<>();
            }
                
            PositionList positionList = positionsOpt.get();
            Set<DocSentenceMatch> matches = new HashSet<>();
            
            // Process positions based on granularity
            if (granularity == Query.Granularity.DOCUMENT) {
                Map<Integer, DocSentenceMatch> documentMatches = new HashMap<>();
                
                for (Position position : positionList.getPositions()) {
                    int docId = position.getDocumentId();
                    DocSentenceMatch match = documentMatches.computeIfAbsent(docId, 
                        id -> new DocSentenceMatch(id));
                    
                    // Add the position to the match
                    match.addPosition(isVariable ? variableName : "match", position);
                    
                    // Bind variable if this is a variable binding
                    if (isVariable) {
                        bindVariable(variableName, normalizedPattern, position, bindingContext);
                    }
                }
                
                matches.addAll(documentMatches.values());
            } else {
                Map<SentenceKey, DocSentenceMatch> sentenceMatches = new HashMap<>();
                
                for (Position position : positionList.getPositions()) {
                    int docId = position.getDocumentId();
                    int sentId = position.getSentenceId();
                    
                    SentenceKey key = new SentenceKey(docId, sentId);
                    DocSentenceMatch match = sentenceMatches.computeIfAbsent(key, 
                        k -> new DocSentenceMatch(docId, sentId));
                    
                    // Add the position to the match
                    match.addPosition(isVariable ? variableName : "match", position);
                    
                    // Bind variable if this is a variable binding
                    if (isVariable) {
                        bindVariable(variableName, normalizedPattern, position, bindingContext);
                    }
                }
                
                matches.addAll(sentenceMatches.values());
            }
            
            return matches;
        } catch (Exception e) {
            throw new QueryExecutionException(
                "Error searching for pattern: " + e.getMessage(),
                e,
                "CONTAINS(" + pattern + ")",
                QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR
            );
        }
    }
    
    /**
     * Binds a variable for a text span.
     *
     * @param variableName The variable name
     * @param term The matched term
     * @param position The position of the match
     * @param bindingContext The binding context to update
     */
    private void bindVariable(String variableName, String term, Position position, 
                             BindingContext bindingContext) {
        if (variableName == null) {
            return;
        }
        
        // Make sure variable name has ? prefix
        String formattedVarName = variableName.startsWith("?") ? variableName : "?" + variableName;
        
        // Create a text span value
        TextSpan span = new TextSpan(term, position.getBeginPosition(), position.getEndPosition());
        
        // Add to binding context
        bindingContext.bindValue(formattedVarName, span);
    }
    
    /**
     * Represents a text span with position information.
     * This is used as the value type for variable bindings.
     */
    public record TextSpan(String text, int beginPosition, int endPosition) {
        @Override
        public String toString() {
            return String.format("%s@%d:%d", text, beginPosition, endPosition);
        }
    }
    
    /**
     * Helper record for sentence identification.
     * This is a duplicate of the one in ConditionExecutor to maintain compatibility.
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