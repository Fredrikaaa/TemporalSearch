package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DependencyCondition;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for DependencyCondition.
 * Searches for syntactic dependencies between words in the dependency index.
 */
public class DependencyConditionExecutor implements ConditionExecutor<DependencyCondition> {
    private static final Logger logger = LoggerFactory.getLogger(DependencyConditionExecutor.class);
    
    private static final String DEPENDENCY_INDEX = "dependency";

    @Override
    public Set<DocSentenceMatch> execute(DependencyCondition condition, Map<String, IndexAccess> indexes,
                               VariableBindings variableBindings, Query.Granularity granularity)
        throws QueryExecutionException {
        
        String governor = condition.getGovernor();
        String relation = condition.getRelation();
        String dependent = condition.getDependent();
        
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
                    
                    // Get or create the document match
                    DocSentenceMatch match = docMatches.computeIfAbsent(docId, 
                            id -> new DocSentenceMatch(id));
                    
                    // Add the position to the match - use original terms for display
                    String originalKey = governor + ":" + relation + ":" + dependent;
                    match.addPosition(originalKey, position);
                }
                
                matches.addAll(docMatches.values());
            } else {
                // Sentence granularity - group by document ID and sentence ID
                Map<String, DocSentenceMatch> sentMatches = new HashMap<>();
                
                for (Position position : positionList.getPositions()) {
                    int docId = position.getDocumentId();
                    int sentId = position.getSentenceId();
                    String key = docId + ":" + sentId;
                    
                    // Get or create the sentence match
                    DocSentenceMatch match = sentMatches.computeIfAbsent(key, 
                            k -> new DocSentenceMatch(docId, sentId));
                    
                    // Add the position to the match - use original terms for display
                    String originalKey = governor + ":" + relation + ":" + dependent;
                    match.addPosition(originalKey, position);
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
} 