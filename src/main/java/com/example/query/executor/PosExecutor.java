package com.example.query.executor;

import com.example.core.IndexAccessInterface;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.Query;
import com.example.query.model.condition.Pos;
import com.example.query.binding.MatchDetail;
import com.example.query.binding.ValueType;
import com.example.query.executor.QueryResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for POS (Part of Speech) conditions.
 * Handles part-of-speech pattern matching and variable binding.
 * Returns QueryResult containing MatchDetail objects.
 */
public final class PosExecutor implements ConditionExecutor<Pos> {
    private static final Logger logger = LoggerFactory.getLogger(PosExecutor.class);
    
    private static final String POS_INDEX = "pos";
    private static final String POS_TERM_DELIMITER = "/"; // Delimiter for value string
    
    /**
     * Creates a new POS executor.
     */
    public PosExecutor() {
        // No initialization required
    }

    @Override
    public QueryResult execute(Pos condition, Map<String, IndexAccessInterface> indexes,
                               Query.Granularity granularity,
                               int granularitySize,
                               String corpusName)
        throws QueryExecutionException {
        
        logger.debug("Executing POS condition for tag {} at {} granularity with size {} (corpus: {})", 
                condition.posTag(), granularity, granularitySize, corpusName);
        
        // Validate required indexes
        if (!indexes.containsKey(POS_INDEX)) {
            throw new QueryExecutionException(
                "Missing required POS index",
                condition.toString(),
                QueryExecutionException.ErrorType.MISSING_INDEX
            );
        }
        
        String posTag = condition.posTag();
        String term = condition.term();
        boolean isVariable = condition.isVariable();
        String variableName = condition.variableName();
        
        // Normalize POS tag to lowercase
        String normalizedPosTag = posTag.toLowerCase();
        
        // Normalize term to lowercase if it's present and not a variable extraction
        String normalizedTerm = null;
        if (term != null && !isVariable) {
            normalizedTerm = term.toLowerCase();
        }
        
        logger.debug("POS condition details: tag='{}', term='{}', isVariable={}, variableName='{}'",
                    normalizedPosTag, normalizedTerm != null ? normalizedTerm : "(any)", isVariable, variableName);
        
        // Get the POS index
        IndexAccessInterface index = indexes.get(POS_INDEX);
        
        if (index == null) {
            throw new QueryExecutionException(
                "Required index not found: " + POS_INDEX,
                condition.toString(),
                QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR
            );
        }
        
        List<MatchDetail> details; // Collect MatchDetail first
        
        try {
            if (isVariable) {
                // Variable binding mode - extract terms with the given POS tag
                details = executeVariableExtraction(normalizedPosTag, variableName, index, condition);
            } else {
                // Search mode - find documents with specific term and POS tag
                if (normalizedTerm == null) { // Should not happen if !isVariable, but check anyway
                     throw new QueryExecutionException(
                        "Term cannot be null when not in variable extraction mode for POS condition",
                        condition.toString(), QueryExecutionException.ErrorType.INVALID_CONDITION);
                }
                details = executeTermSearch(normalizedPosTag, normalizedTerm, index, condition);
            }
            
            logger.debug("POS condition produced {} MatchDetail objects. Returning QueryResult.",
                        details.size());
            
            // Create QueryResult directly
            QueryResult finalResult = new QueryResult(granularity, granularitySize, details);

            logger.debug("POS execution complete with {} MatchDetail objects.", finalResult.getAllDetails().size());
            return finalResult;
        } catch (Exception e) {
            // Catch specific QueryExecutionException first if needed
            if (e instanceof QueryExecutionException qee) {
                throw qee;
            }
            throw new QueryExecutionException(
                "Error executing POS condition: " + e.getMessage(),
                e,
                condition.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Executes a variable extraction for a specific POS tag.
     * This mode extracts all terms with the given POS tag.
     *
     * @param posTag The normalized POS tag to extract
     * @param variableName The variable name to associate with the MatchDetail
     * @param index The index to search in
     * @param condition The original condition object (for ID)
     * @return List of MatchDetail objects
     */
    private List<MatchDetail> executeVariableExtraction(String posTag, String variableName, IndexAccessInterface index,
                                                  Pos condition)
        throws Exception {
        
        logger.debug("Extracting all terms with POS tag '{}' for variable '{}'",
                    posTag, variableName);
        
        List<MatchDetail> details = new ArrayList<>();
        String conditionId = String.valueOf(condition.hashCode());
        
        // Use a prefix search with the POS tag
        String prefix = posTag + IndexAccessInterface.DELIMITER;
        byte[] prefixBytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        
        try (var iterator = index.iterator()) {
            iterator.seek(prefixBytes);
            
            while (iterator.hasNext()) {
                // Remove peekNext(), use standard iterator pattern
                // Entry<byte[], byte[]> currentEntry = iterator.peekNext(); 
                Entry<byte[], byte[]> currentEntry = iterator.next(); // Get entry using next()
                
                byte[] keyBytes = currentEntry.getKey();
                byte[] valueBytes = currentEntry.getValue();
                
                String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                if (!key.startsWith(prefix)) {
                    break; // Moved past this POS tag
                }
                
                // iterator.next(); // Consume entry AFTER processing peeked entry <-- REMOVE THIS LINE TOO
                
                // Extract term (part after delimiter)
                String termPart = key.substring(prefix.length());
                PositionList positions = PositionList.deserialize(valueBytes);
                String valueString = termPart + POS_TERM_DELIMITER + posTag; // Format: term/tag
                
                for (Position position : positions.getPositions()) {
                    // Use the 5-arg constructor for non-join results
                    MatchDetail detail = new MatchDetail(
                        valueString,       // Value is "term/tag"
                        ValueType.POS_TERM,
                        position,
                        conditionId,
                        variableName       // Associate with the variable
                    );
                    details.add(detail);
                }
            }
        }
        
        logger.debug("Extracted {} details for POS tag '{}'", details.size(), posTag);
        return details;
    }
    
    /**
     * Executes a term search for a specific term with a specific POS tag.
     *
     * @param posTag The normalized POS tag to search for
     * @param term The normalized term to search for
     * @param index The index to search in
     * @param condition The original condition object (for ID)
     * @return List of MatchDetail objects
     */
    private List<MatchDetail> executeTermSearch(String posTag, String term, IndexAccessInterface index,
                                           Pos condition)
        throws Exception {
        
        // Use original term/tag from condition for the MatchDetail value, 
        // but normalized versions for the search key.
        String originalTerm = condition.term(); 
        String originalTag = condition.posTag();

        logger.debug("Searching for term '{}' with POS tag '{}'", term, posTag);
        String conditionId = String.valueOf(condition.hashCode());
        
        String searchKey = posTag + IndexAccessInterface.DELIMITER + term;
        byte[] keyBytes = searchKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        
        Optional<PositionList> positionsOpt = index.get(keyBytes);
        
        if (!positionsOpt.isPresent()) {
            logger.debug("Term '{}' with POS tag '{}' not found", term, posTag);
            return Collections.emptyList(); // Return empty list
        }
        
        PositionList positionList = positionsOpt.get();
        // Construct value using original case from condition object
        String valueString = originalTerm + POS_TERM_DELIMITER + originalTag; 
        
        // Create MatchDetail for each position first
        List<MatchDetail> allFoundDetails = new ArrayList<>();
        for (Position position : positionList.getPositions()) {
            // Use the 5-arg constructor for non-join results
            MatchDetail detail = new MatchDetail(
                valueString,       // Value is "term/tag"
                ValueType.POS_TERM,
                position,
                conditionId,
                null               // No variable name in term search mode
            );
            allFoundDetails.add(detail);
        }
        
        // Return the collected details directly. The QueryExecutor or Result service
        // should handle potential aggregation or uniqueness based on granularity later if needed.
        // For now, the executor returns all raw position matches found for the key.
        logger.debug("Found {} raw details for term '{}' with POS tag '{}'",
                    allFoundDetails.size(), term, posTag);
        return allFoundDetails;
    }
} 