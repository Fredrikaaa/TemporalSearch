package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Ner;
import com.example.query.binding.BindingContext;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for NER (Named Entity Recognition) conditions.
 * Handles entity type matching and variable binding for named entities.
 */
public final class NerExecutor implements ConditionExecutor<Ner> {
    private static final Logger logger = LoggerFactory.getLogger(NerExecutor.class);
    
    private static final String NER_INDEX = "ner";
    private static final String DATE_INDEX = "ner_date";

    /**
     * Creates a new NER executor.
     */
    public NerExecutor() {
        // No initialization required
    }

    @Override
    public Set<DocSentenceMatch> execute(Ner condition, Map<String, IndexAccess> indexes,
                               BindingContext bindingContext, Query.Granularity granularity,
                               int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing NER condition for type {} at {} granularity with size {}", 
                condition.entityType(), granularity, granularitySize);
        
        // Validate required indexes
        String requiredIndex = condition.entityType().equals("DATE") ? DATE_INDEX : NER_INDEX;
        if (!indexes.containsKey(requiredIndex)) {
            throw new QueryExecutionException(
                String.format("Missing required index: %s", requiredIndex),
                condition.toString(),
                QueryExecutionException.ErrorType.MISSING_INDEX
            );
        }
        
        String entityType = condition.entityType();
        String variableName = condition.variableName();
        boolean isVariable = condition.isVariable();
        
        // Normalize entity type to uppercase for standard types like DATE, PERSON, etc.
        // This is an exception to the lowercase rule since entity types are conventionally uppercase
        String normalizedEntityType = entityType;
        if (!"*".equals(entityType)) {
            normalizedEntityType = entityType.toUpperCase();
        }
        
        // Determine which index to use (DATE uses separate index)
        String indexName = "DATE".equals(normalizedEntityType) ? DATE_INDEX : NER_INDEX;
        IndexAccess index = indexes.get(indexName);
        
        if (index == null) {
            throw new QueryExecutionException(
                "Required index not found: " + indexName,
                condition.toString(),
                QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR
            );
        }
        
        Set<DocSentenceMatch> result = new HashSet<>();
        
        try {
            if (isVariable) {
                // Variable binding mode - extract entities of the given type
                result = executeVariableExtraction(normalizedEntityType, variableName, index, bindingContext, granularity);
                
                // Add debug logging to check variable bindings
                logger.debug("After variable extraction for type '{}', variable '{}', binding context: {}", 
                             normalizedEntityType, variableName, bindingContext);
            } else {
                // Search mode - find documents with specific entity type
                result = executeEntitySearch(normalizedEntityType, index, granularity);
            }
            
            logger.debug("NER condition matched {} results at {} granularity", 
                        result.size(), granularity);
            return result;
        } catch (Exception e) {
            throw new QueryExecutionException(
                "Error executing NER condition: " + e.getMessage(),
                e,
                condition.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Executes variable extraction for a specific entity type.
     * This mode finds all entities of the given type and binds them to a variable.
     *
     * @param entityType The entity type to extract
     * @param variableName The variable name to bind entities to
     * @param index The index to search in
     * @param bindingContext The binding context for variable bindings
     * @param granularity Whether to return document or sentence level matches
     * @return Set of document/sentence matches with entity bindings
     */
    private Set<DocSentenceMatch> executeVariableExtraction(String entityType, String variableName, IndexAccess index,
                                                  BindingContext bindingContext, Query.Granularity granularity) 
        throws Exception {
        
        logger.debug("Extracting all entities of type '{}' and binding to variable '{}' at {} granularity", 
                    entityType, variableName, granularity);
        
        // Ensure variable name is properly formatted
        variableName = ensureVariableName(variableName);
        
        Set<DocSentenceMatch> matches = new HashSet<>();
        
        // For DATE entities
        if ("DATE".equals(entityType)) {
            // For dates, we need to iterate through the index to find all date entities
            try (var iterator = index.iterator()) {
                iterator.seekToFirst();
                
                while (iterator.hasNext()) {
                    byte[] keyBytes = iterator.peekNext().getKey();
                    byte[] valueBytes = iterator.peekNext().getValue();
                    iterator.next();
                    
                    // Get date value from key
                    String dateStr = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                    PositionList positions = PositionList.deserialize(valueBytes);
                    
                    // Bind date value to variable for each document/sentence
                    for (Position position : positions.getPositions()) {
                        int docId = position.getDocumentId();
                        int sentenceId = position.getSentenceId();
                        int beginPosition = position.getBeginPosition();
                        int endPosition = position.getEndPosition();
                        
                        // Create a match with tracking info
                        DocSentenceMatch match;
                        if (granularity == Query.Granularity.DOCUMENT) {
                            match = new DocSentenceMatch(docId);
                        } else {
                            match = new DocSentenceMatch(docId, sentenceId);
                        }
                        
                        // Format the value with position information
                        MatchedEntityValue entityValue = new MatchedEntityValue(dateStr, beginPosition, endPosition, docId, sentenceId);
                        
                        // Add the binding to the context (global)
                        bindingContext.bindValue(variableName, entityValue);
                        
                        // Also set the value directly on the match object (local)
                        match.setVariableValue(variableName, entityValue);
                        
                        logger.debug("Bound DATE value '{}' at doc:{}, sent:{}, pos:{}:{} to {}", 
                                    dateStr, docId, sentenceId, beginPosition, endPosition, variableName);
                        
                        // Add match to results
                        matches.add(match);
                    }
                }
            }
        } else {
            // For non-DATE entities, use the consolidated NER index
            // We'll use a prefix search with the entity type
            String prefix = entityType + IndexAccess.NGRAM_DELIMITER;
            byte[] prefixBytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            
            try (var iterator = index.iterator()) {
                // Start from the entity type prefix
                iterator.seek(prefixBytes);
                
                while (iterator.hasNext()) {
                    byte[] keyBytes = iterator.peekNext().getKey();
                    byte[] valueBytes = iterator.peekNext().getValue();
                    
                    // Check if we're still in the same entity type
                    String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                    if (!key.startsWith(prefix)) {
                        break; // We've moved past this entity type
                    }
                    
                    iterator.next(); // Move to next entry
                    
                    // Extract entity value (remove prefix)
                    String entityText = key.substring(prefix.length());
                    
                    // Log the raw entity text found in the index
                    logger.debug("Found raw {} entity in index: '{}'", entityType, entityText);
                    
                    PositionList positions = PositionList.deserialize(valueBytes);
                    
                    // Bind entity value to variable for each document/sentence
                    for (Position position : positions.getPositions()) {
                        int docId = position.getDocumentId();
                        int sentenceId = position.getSentenceId();
                        int beginPos = position.getBeginPosition();
                        int endPos = position.getEndPosition();
                        
                        // Create a match with tracking info
                        DocSentenceMatch match;
                        if (granularity == Query.Granularity.DOCUMENT) {
                            match = new DocSentenceMatch(docId);
                        } else {
                            match = new DocSentenceMatch(docId, sentenceId);
                        }
                        
                        // Create entity value object with document and sentence IDs
                        MatchedEntityValue entityValue = new MatchedEntityValue(entityText, beginPos, endPos, docId, sentenceId);
                        
                        // Add the binding to the global context
                        bindingContext.bindValue(variableName, entityValue);
                        
                        // Also set the value directly on the match object (local)
                        match.setVariableValue(variableName, entityValue);
                        
                        logger.debug("Bound {} value '{}' at doc:{}, sent:{}, pos:{}:{} to {}", 
                                    entityType, entityText, docId, sentenceId, beginPos, endPos, variableName);
                        
                        // Add match to results
                        matches.add(match);
                    }
                }
            }
        }
        
        logger.debug("Extracted entities of type '{}' from {} results at {} granularity", 
                    entityType, matches.size(), granularity);
        return matches;
    }
    
    /**
     * Executes an entity search for a specific entity type.
     * This mode just finds documents that contain any entity of the given type.
     *
     * @param entityType The entity type to search for
     * @param index The index to search in
     * @param granularity Whether to return document or sentence level matches
     * @return Set of matches at the specified granularity level
     */
    private Set<DocSentenceMatch> executeEntitySearch(String entityType, IndexAccess index,
                                                     Query.Granularity granularity)
        throws Exception {
        
        logger.debug("Searching for entity type '{}' at {} granularity", 
                    entityType, granularity);
        
        // Create the search key prefix for the entity type
        String searchKey;
        if ("DATE".equals(entityType)) {
            // For DATE index, we need to iterate through all entries
            return getAllDateMatches(index, granularity);
        } else {
            // For other entity types, use prefix search
            searchKey = entityType + IndexAccess.NGRAM_DELIMITER;
        }
        
        Set<DocSentenceMatch> matches = new HashSet<>();
        
        // Use prefix search to find all entities of this type
        byte[] prefixBytes = searchKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (var iterator = index.iterator()) {
            iterator.seek(prefixBytes);
            
            while (iterator.hasNext()) {
                byte[] keyBytes = iterator.peekNext().getKey();
                byte[] valueBytes = iterator.peekNext().getValue();
                
                // Check if we're still in the same entity type
                String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                if (!key.startsWith(searchKey)) {
                    break; // We've moved past this entity type
                }
                
                iterator.next(); // Move to next entry
                
                // Add matches from this entry
                PositionList positions = PositionList.deserialize(valueBytes);
                for (Position position : positions.getPositions()) {
                    int docId = position.getDocumentId();
                    int sentenceId = position.getSentenceId();
                    
                    if (granularity == Query.Granularity.DOCUMENT) {
                        matches.add(new DocSentenceMatch(docId));
                    } else {
                        matches.add(new DocSentenceMatch(docId, sentenceId));
                    }
                }
            }
        }
        
        logger.debug("Found {} results for entity type '{}' at {} granularity", 
                    matches.size(), entityType, granularity);
        return matches;
    }

    /**
     * Gets all matches from the DATE index.
     */
    private Set<DocSentenceMatch> getAllDateMatches(IndexAccess index, Query.Granularity granularity) 
        throws Exception {
        Set<DocSentenceMatch> matches = new HashSet<>();
        
        try (var iterator = index.iterator()) {
            iterator.seekToFirst();
            
            while (iterator.hasNext()) {
                byte[] valueBytes = iterator.peekNext().getValue();
                iterator.next();
                
                PositionList positions = PositionList.deserialize(valueBytes);
                for (Position position : positions.getPositions()) {
                    int docId = position.getDocumentId();
                    int sentenceId = position.getSentenceId();
                    
                    if (granularity == Query.Granularity.DOCUMENT) {
                        matches.add(new DocSentenceMatch(docId));
                    } else {
                        matches.add(new DocSentenceMatch(docId, sentenceId));
                    }
                }
            }
        }
        
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
        return variableName.startsWith("?") ? variableName : "?" + variableName;
    }
    
    /**
     * Represents an entity value with position information.
     * This is used as the value type for variable bindings.
     */
    public record MatchedEntityValue(String text, int beginPosition, int endPosition, int documentId, int sentenceId) {
        @Override
        public String toString() {
            return String.format("%s@%d:%d", text, beginPosition, endPosition);
        }
    }
} 