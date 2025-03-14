package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Contains;
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

    private final String variableName;

    public ContainsExecutor(String variableName) {
        this.variableName = variableName;
    }

    @Override
    public Set<DocSentenceMatch> execute(Contains condition, Map<String, IndexAccess> indexes,
                               VariableBindings variableBindings, Query.Granularity granularity,
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
                    pattern, isVariable, variableName, index, variableBindings, granularity);
                
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
     * @param variableBindings The variable bindings to update
     * @param granularity The granularity of the search
     * @return Set of matches at the specified granularity level
     */
    private Set<DocSentenceMatch> executePatternSearch(String pattern, boolean isVariable, String variableName,
                                        IndexAccess index, VariableBindings variableBindings, 
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
                // Document granularity - group by document ID
                Map<Integer, DocSentenceMatch> docMatches = new HashMap<>();
                
                for (Position position : positionList.getPositions()) {
                    addDocumentMatch(position, docMatches, variableName);
                    
                    // Handle variable binding
                    if (isVariable) {
                        bindVariable(variableName, pattern, position, variableBindings);
                    }
                }
                
                matches.addAll(docMatches.values());
            } else {
                // Sentence granularity - group by document ID and sentence ID
                Map<SentenceKey, DocSentenceMatch> sentMatches = new HashMap<>();
                
                for (Position position : positionList.getPositions()) {
                    addSentenceMatch(position, sentMatches, variableName);
                    
                    // Handle variable binding
                    if (isVariable) {
                        bindVariable(variableName, pattern, position, variableBindings);
                    }
                }
                
                matches.addAll(sentMatches.values());
            }
            
            logger.debug("Found pattern '{}' in {} {}", normalizedPattern, matches.size(), 
                        granularity == Query.Granularity.DOCUMENT ? "documents" : "sentences");
                
            return matches;
        } catch (Exception e) {
            throw new QueryExecutionException(
                "Error executing CONTAINS condition for pattern '" + normalizedPattern + "': " + e.getMessage(),
                e,
                "CONTAINS(" + pattern + ")",
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Binds a variable to a position.
     *
     * @param variableName The name of the variable to bind
     * @param term The term that matched
     * @param position The position where the term was found
     * @param variableBindings The variable bindings to update
     */
    private void bindVariable(String variableName, String term, Position position, 
                             VariableBindings variableBindings) {
        int docId = position.getDocumentId();
        int sentenceId = position.getSentenceId();
        int beginPos = position.getBeginPosition();
        int endPos = position.getEndPosition();
        
        // Format: term@beginPos:endPos
        // This new format makes it easier to use character positions directly for highlighting
        String valueWithPosition = term + "@" + beginPos + ":" + endPos;
        
        variableBindings.addBinding(docId, variableName, valueWithPosition);
        logger.debug("Bound variable '{}' to '{}' in document {}", 
                    variableName, valueWithPosition, docId);
    }
} 