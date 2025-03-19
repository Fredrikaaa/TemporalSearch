package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Pos;
import com.example.query.binding.BindingContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for POS (Part of Speech) conditions.
 * Handles part-of-speech pattern matching and variable binding.
 */
public final class PosExecutor implements ConditionExecutor<Pos> {
    private static final Logger logger = LoggerFactory.getLogger(PosExecutor.class);
    
    private static final String POS_INDEX = "pos";
    
    /**
     * Creates a new POS executor.
     */
    public PosExecutor() {
        // No initialization required
    }

    @Override
    public Set<DocSentenceMatch> execute(Pos condition, Map<String, IndexAccess> indexes,
                               BindingContext bindingContext, Query.Granularity granularity,
                               int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing POS condition for tag {} at {} granularity with size {}", 
                condition.posTag(), granularity, granularitySize);
        
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
        
        // Normalize term to lowercase if it's not a variable
        String normalizedTerm = term;
        if (!isVariable) {
            normalizedTerm = term.toLowerCase();
        }
        
        logger.debug("Executing POS condition for tag '{}' (normalized: '{}'), term '{}' (normalized: '{}'), isVariable={}, granularity={}", 
                    posTag, normalizedPosTag, term, normalizedTerm, isVariable, granularity);
        
        // Get the POS index
        IndexAccess index = indexes.get(POS_INDEX);
        
        if (index == null) {
            throw new QueryExecutionException(
                "Required index not found: " + POS_INDEX,
                condition.toString(),
                QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR
            );
        }
        
        Set<DocSentenceMatch> result = new HashSet<>();
        
        try {
            if (isVariable) {
                // Variable binding mode - extract terms with the given POS tag
                result = executeVariableExtraction(normalizedPosTag, variableName, index, bindingContext, granularity);
            } else {
                // Search mode - find documents with specific term and POS tag
                result = executeTermSearch(normalizedPosTag, normalizedTerm, index, granularity);
            }
            
            logger.debug("POS condition matched {} results at {} granularity", 
                        result.size(), granularity);
            return result;
        } catch (Exception e) {
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
     * This mode extracts all terms with the given POS tag and binds them to the variable.
     *
     * @param posTag The POS tag to extract
     * @param variableName The variable name to bind to
     * @param index The index to search in
     * @param bindingContext The binding context to update
     * @param granularity Whether to return document or sentence level matches
     * @return Set of matches at the specified granularity level
     */
    private Set<DocSentenceMatch> executeVariableExtraction(String posTag, String variableName, IndexAccess index,
                                                  BindingContext bindingContext, Query.Granularity granularity) 
        throws Exception {
        
        logger.debug("Extracting all terms with POS tag '{}' and binding to variable '{}' at {} granularity", 
                    posTag, variableName, granularity);
        
        // Ensure variable name is properly formatted
        String formattedVarName = ensureVariableName(variableName);
        
        Set<DocSentenceMatch> matches = new HashSet<>();
        
        // Use a prefix search with the POS tag
        String prefix = posTag + IndexAccess.NGRAM_DELIMITER;
        byte[] prefixBytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        
        try (var iterator = index.iterator()) {
            // Start from the POS tag prefix
            iterator.seek(prefixBytes);
            
            while (iterator.hasNext()) {
                byte[] keyBytes = iterator.peekNext().getKey();
                byte[] valueBytes = iterator.peekNext().getValue();
                
                // Check if we're still in the same POS tag
                String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                if (!key.startsWith(prefix)) {
                    break; // We've moved past this POS tag
                }
                
                iterator.next(); // Move to next entry
                
                // Extract term (remove prefix)
                String term = key.substring(prefix.length());
                PositionList positions = PositionList.deserialize(valueBytes);
                
                // Bind term to variable for each document/sentence
                for (Position position : positions.getPositions()) {
                    int docId = position.getDocumentId();
                    int sentenceId = position.getSentenceId();
                    int beginPos = position.getBeginPosition();
                    int endPos = position.getEndPosition();
                    
                    // Create text span value
                    TextSpan span = new TextSpan(term, beginPos, endPos);
                    
                    // Add binding to context
                    bindingContext.bindValue(formattedVarName, span);
                    
                    // Add match to results
                    if (granularity == Query.Granularity.DOCUMENT) {
                        matches.add(new DocSentenceMatch(docId));
                    } else {
                        matches.add(new DocSentenceMatch(docId, sentenceId));
                    }
                }
            }
        }
        
        logger.debug("Extracted terms with POS tag '{}' from {} results at {} granularity", 
                    posTag, matches.size(), granularity);
        return matches;
    }
    
    /**
     * Executes a term search for a specific term with a specific POS tag.
     *
     * @param posTag The POS tag to search for
     * @param term The term to search for
     * @param index The index to search in
     * @param granularity Whether to return document or sentence level matches
     * @return Set of matches at the specified granularity level
     */
    private Set<DocSentenceMatch> executeTermSearch(String posTag, String term, IndexAccess index,
                                           Query.Granularity granularity)
        throws Exception {
        
        logger.debug("Searching for term '{}' with POS tag '{}' at {} granularity", 
                    term, posTag, granularity);
        
        // Create the search key in format "posTag\0term" using null byte delimiter
        String searchKey = posTag + IndexAccess.NGRAM_DELIMITER + term;
        
        // Convert to bytes
        byte[] keyBytes = searchKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        
        // Search the index
        Optional<PositionList> positionsOpt = index.get(keyBytes);
        
        if (!positionsOpt.isPresent()) {
            logger.debug("Term '{}' with POS tag '{}' not found in any documents", term, posTag);
            return new HashSet<>();
        }
        
        PositionList positionList = positionsOpt.get();
        Set<DocSentenceMatch> matches = new HashSet<>();
        
        // Process positions based on granularity
        if (granularity == Query.Granularity.DOCUMENT) {
            // Document granularity - group by document ID
            Map<Integer, DocSentenceMatch> docMatches = new HashMap<>();
            
            for (Position position : positionList.getPositions()) {
                int docId = position.getDocumentId();
                DocSentenceMatch match = docMatches.computeIfAbsent(docId, 
                    id -> new DocSentenceMatch(id));
                
                // Add the position to the match with a standard key
                match.addPosition("match", position);
            }
            
            matches.addAll(docMatches.values());
        } else {
            // Sentence granularity - group by document ID and sentence ID
            Map<SentenceKey, DocSentenceMatch> sentMatches = new HashMap<>();
            
            for (Position position : positionList.getPositions()) {
                int docId = position.getDocumentId();
                int sentId = position.getSentenceId();
                
                SentenceKey key = new SentenceKey(docId, sentId);
                DocSentenceMatch match = sentMatches.computeIfAbsent(key, 
                    k -> new DocSentenceMatch(docId, sentId));
                
                // Add the position to the match with a standard key
                match.addPosition("match", position);
            }
            
            matches.addAll(sentMatches.values());
        }
        
        logger.debug("Found term '{}' with POS tag '{}' in {} {}", term, posTag, matches.size(), 
                granularity == Query.Granularity.DOCUMENT ? "documents" : "sentences");
        
        return matches;
    }
    
    /**
     * Ensures that a variable name starts with ?.
     * This is a utility method to normalize variable names.
     * 
     * @param variableName The variable name to check
     * @return The variable name with ? prefix if needed
     */
    private String ensureVariableName(String variableName) {
        if (variableName == null) {
            return null;
        }
        return variableName.startsWith("?") ? variableName : "?" + variableName;
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