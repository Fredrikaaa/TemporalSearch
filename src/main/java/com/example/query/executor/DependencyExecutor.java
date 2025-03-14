package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Dependency;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for dependency conditions.
 * Handles syntactic dependency pattern matching and variable binding.
 */
public final class DependencyExecutor implements ConditionExecutor<Dependency> {
    private static final Logger logger = LoggerFactory.getLogger(DependencyExecutor.class);
    
    private static final String DEPENDENCY_INDEX = "dependency";
    private final String variableName;

    public DependencyExecutor(String variableName) {
        this.variableName = variableName;
    }

    @Override
    public Set<DocSentenceMatch> execute(Dependency condition, Map<String, IndexAccess> indexes,
                               VariableBindings variableBindings, Query.Granularity granularity,
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
                    addDocumentMatch(position, docMatches, variableName);
                }
                
                matches.addAll(docMatches.values());
            } else {
                // Sentence granularity - group by document ID and sentence ID
                Map<SentenceKey, DocSentenceMatch> sentMatches = new HashMap<>();
                
                for (Position position : positionList.getPositions()) {
                    addSentenceMatch(position, sentMatches, variableName);
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