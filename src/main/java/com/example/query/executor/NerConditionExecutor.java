package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.NerCondition;
import com.example.query.model.Query;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for NerCondition.
 * Searches for named entities in the NER indexes.
 * Uses the consolidated NER index for all entity types except DATE, which uses a dedicated index.
 */
public class NerConditionExecutor implements ConditionExecutor<NerCondition> {
    private static final Logger logger = LoggerFactory.getLogger(NerConditionExecutor.class);
    
    private static final String NER_INDEX = "ner";
    private static final String DATE_INDEX = "ner_date";

    @Override
    public Set<DocSentenceMatch> execute(NerCondition condition, Map<String, IndexAccess> indexes,
                               VariableBindings variableBindings, Query.Granularity granularity)
        throws QueryExecutionException {
        
        String entityType = condition.getEntityType();
        String target = condition.getTarget();
        boolean isVariable = condition.isVariable();
        
        // Normalize entity type to uppercase for standard types like DATE, PERSON, etc.
        // This is an exception to the lowercase rule since entity types are conventionally uppercase
        String normalizedEntityType = entityType;
        if (!"*".equals(entityType)) {
            normalizedEntityType = entityType.toUpperCase();
        }
        
        // Normalize target to lowercase if it's not a variable
        String normalizedTarget = target;
        if (!isVariable) {
            normalizedTarget = target.toLowerCase();
        }
        
        logger.debug("Executing NER condition for entity type '{}' (normalized: '{}'), target '{}' (normalized: '{}'), isVariable={}, granularity={}", 
                    entityType, normalizedEntityType, target, normalizedTarget, isVariable, granularity);
        
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
                result = executeVariableExtraction(normalizedEntityType, normalizedTarget, index, variableBindings, granularity);
            } else {
                // Search mode - find documents with specific entity
                result = executeEntitySearch(normalizedEntityType, normalizedTarget, index, granularity);
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
     * Executes a variable extraction for a specific entity type.
     * This mode extracts all entities of the given type and binds them to the variable.
     *
     * @param entityType The entity type to extract
     * @param variableName The variable name to bind to (without the ? prefix)
     * @param index The index to search in
     * @param variableBindings The variable bindings to update
     * @param granularity Whether to return document or sentence level matches
     * @return Set of matches at the specified granularity level
     */
    private Set<DocSentenceMatch> executeVariableExtraction(String entityType, String variableName, IndexAccess index,
                                                  VariableBindings variableBindings, Query.Granularity granularity) 
        throws Exception {
        
        logger.debug("Extracting all entities of type '{}' and binding to variable '{}' at {} granularity", 
                    entityType, variableName, granularity);
        
        // Remove the ? from variable name if present
        if (variableName.startsWith("?")) {
            variableName = variableName.substring(1);
        }
        
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
                        
                        variableBindings.addBinding(docId, variableName, dateStr);
                        
                        if (granularity == Query.Granularity.DOCUMENT) {
                            matches.add(new DocSentenceMatch(docId));
                        } else {
                            matches.add(new DocSentenceMatch(docId, sentenceId));
                        }
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
                    String entityValue = key.substring(prefix.length());
                    PositionList positions = PositionList.deserialize(valueBytes);
                    
                    // Bind entity value to variable for each document/sentence
                    for (Position position : positions.getPositions()) {
                        int docId = position.getDocumentId();
                        int sentenceId = position.getSentenceId();
                        
                        variableBindings.addBinding(docId, variableName, entityValue);
                        
                        if (granularity == Query.Granularity.DOCUMENT) {
                            matches.add(new DocSentenceMatch(docId));
                        } else {
                            matches.add(new DocSentenceMatch(docId, sentenceId));
                        }
                    }
                }
            }
        }
        
        logger.debug("Extracted entities of type '{}' from {} results at {} granularity", 
                    entityType, matches.size(), granularity);
        return matches;
    }
    
    /**
     * Executes an entity search for a specific entity value.
     *
     * @param entityType The entity type to search for
     * @param entityValue The entity value to search for
     * @param index The index to search in
     * @param granularity Whether to return document or sentence level matches
     * @return Set of matches at the specified granularity level
     */
    private Set<DocSentenceMatch> executeEntitySearch(String entityType, String entityValue, IndexAccess index,
                                                     Query.Granularity granularity)
        throws Exception {
        
        logger.debug("Searching for entity type '{}' with value '{}' at {} granularity", 
                    entityType, entityValue, granularity);
        
        // Create the search key
        String searchKey;
        if ("DATE".equals(entityType)) {
            // DATE index stores dates directly
            searchKey = entityValue;
        } else {
            // Consolidated NER index uses format "entityType\0entityValue" with null byte delimiter
            searchKey = entityType + IndexAccess.NGRAM_DELIMITER + entityValue;
        }
        
        // Convert to bytes
        byte[] keyBytes = searchKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        
        // Search the index
        Optional<PositionList> positionsOpt = index.get(keyBytes);
        
        if (!positionsOpt.isPresent()) {
            logger.debug("Entity '{}' not found in any documents", searchKey);
            return new HashSet<>();
        }
        
        PositionList positionList = positionsOpt.get();
        Set<DocSentenceMatch> matches = new HashSet<>();
        
        // Extract document/sentence IDs based on granularity
        for (Position position : positionList.getPositions()) {
            int docId = position.getDocumentId();
            int sentenceId = position.getSentenceId();
            
            if (granularity == Query.Granularity.DOCUMENT) {
                matches.add(new DocSentenceMatch(docId));
            } else {
                matches.add(new DocSentenceMatch(docId, sentenceId));
            }
        }
        
        logger.debug("Found entity '{}' in {} results at {} granularity", 
                    searchKey, matches.size(), granularity);
        return matches;
    }
} 