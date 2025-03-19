package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Dependency;
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
 * Executor for dependency conditions.
 * Handles syntactic dependency pattern matching and variable binding.
 */
public final class DependencyExecutor implements ConditionExecutor<Dependency> {
    private static final Logger logger = LoggerFactory.getLogger(DependencyExecutor.class);
    
    private static final String DEPENDENCY_INDEX = "dependency";

    /**
     * Creates a new dependency executor.
     */
    public DependencyExecutor() {
        // No initialization required
    }

    @Override
    public Set<DocSentenceMatch> execute(Dependency condition, Map<String, IndexAccess> indexes,
                               BindingContext bindingContext, Query.Granularity granularity,
                               int granularitySize)
        throws QueryExecutionException {
        
        logger.debug("Executing dependency condition for relation {} at {} granularity with size {}", 
                condition.relation(), granularity, granularitySize);
        
        // Validate required indexes
        if (!indexes.containsKey(DEPENDENCY_INDEX)) {
            throw new QueryExecutionException(
                "Missing required dependency index",
                condition.toString(),
                QueryExecutionException.ErrorType.MISSING_INDEX
            );
        }
        
        String governor = condition.governor();
        String relation = condition.relation();
        String dependent = condition.dependent();
        String variableName = condition.variableName();
        boolean isVariable = condition.isVariable();
        
        // Normalize all terms to lowercase
        String normalizedGovernor = governor.toLowerCase();
        String normalizedRelation = relation.toLowerCase();
        String normalizedDependent = dependent.toLowerCase();
        
        logger.debug("Executing dependency condition with governor='{}' (normalized='{}'), relation='{}' (normalized='{}'), dependent='{}' (normalized='{}') at {} granularity", 
                    governor, normalizedGovernor, relation, normalizedRelation, dependent, normalizedDependent, granularity);
        
        // Get the dependency index
        IndexAccess index = indexes.get(DEPENDENCY_INDEX);
        
        if (index == null) {
            throw new QueryExecutionException(
                "Required index not found: " + DEPENDENCY_INDEX,
                condition.toString(),
                QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR
            );
        }
        
        try {
            // Create the search key in format "governor\0relation\0dependent" using null byte delimiter
            String searchKey = normalizedGovernor + IndexAccess.NGRAM_DELIMITER + normalizedRelation + IndexAccess.NGRAM_DELIMITER + normalizedDependent;
            
            // Convert to bytes
            byte[] keyBytes = searchKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            
            // Search the index
            Optional<PositionList> positionsOpt = index.get(keyBytes);
            
            if (!positionsOpt.isPresent()) {
                logger.debug("Dependency '{}' not found in any documents", searchKey);
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
                    
                    // Create or get match for this document
                    DocSentenceMatch match = docMatches.computeIfAbsent(docId, 
                        id -> new DocSentenceMatch(id));
                    
                    // Add position to match
                    String positionKey = isVariable ? variableName : "match";
                    match.addPosition(positionKey, position);
                    
                    // Bind dependency value if this is a variable binding
                    if (isVariable) {
                        bindDependency(variableName, governor, relation, dependent, position, bindingContext);
                    }
                }
                
                matches.addAll(docMatches.values());
            } else {
                // Sentence granularity - group by document ID and sentence ID
                Map<SentenceKey, DocSentenceMatch> sentMatches = new HashMap<>();
                
                for (Position position : positionList.getPositions()) {
                    int docId = position.getDocumentId();
                    int sentId = position.getSentenceId();
                    
                    // Create key for this sentence
                    SentenceKey key = new SentenceKey(docId, sentId);
                    
                    // Create or get match for this sentence
                    DocSentenceMatch match = sentMatches.computeIfAbsent(key, 
                        k -> new DocSentenceMatch(docId, sentId));
                    
                    // Add position to match
                    String positionKey = isVariable ? variableName : "match";
                    match.addPosition(positionKey, position);
                    
                    // Bind dependency value if this is a variable binding
                    if (isVariable) {
                        bindDependency(variableName, governor, relation, dependent, position, bindingContext);
                    }
                }
                
                matches.addAll(sentMatches.values());
            }
            
            logger.debug("Found dependency '{}' in {} {}", searchKey, matches.size(), 
                    granularity == Query.Granularity.DOCUMENT ? "documents" : "sentences");
            
            return matches;
        } catch (Exception e) {
            throw new QueryExecutionException(
                "Error executing dependency condition: " + e.getMessage(),
                e,
                condition.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Binds a dependency relationship to a variable.
     *
     * @param variableName The variable name to bind to
     * @param governor The governor term
     * @param relation The dependency relation
     * @param dependent The dependent term
     * @param position The position where the dependency was found
     * @param bindingContext The binding context to update
     */
    private void bindDependency(String variableName, String governor, String relation, String dependent,
                               Position position, BindingContext bindingContext) {
        if (variableName == null) {
            return;
        }
        
        // Ensure variable name has ? prefix
        String formattedVarName = ensureVariableName(variableName);
        
        // Create dependency value
        DependencyValue depValue = new DependencyValue(
            governor, relation, dependent, 
            position.getBeginPosition(), position.getEndPosition()
        );
        
        // Bind value to context
        bindingContext.bindValue(formattedVarName, depValue);
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
     * Represents a dependency relationship with position information.
     * This is used as the value type for variable bindings.
     */
    public record DependencyValue(
        String governor, 
        String relation, 
        String dependent, 
        int beginPosition, 
        int endPosition
    ) {
        @Override
        public String toString() {
            return String.format("%s-%s->%s@%d:%d", 
                governor, relation, dependent, beginPosition, endPosition);
        }
    }
    
    /**
     * Helper record for sentence identification.
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