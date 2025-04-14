package com.example.query.executor;

import com.example.core.IndexAccessInterface;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.core.IndexAccessException;
import com.example.query.model.Query;
import com.example.query.model.condition.Dependency;
import com.example.query.binding.MatchDetail;
import com.example.query.binding.ValueType;
import org.iq80.leveldb.DBIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for DEPENDENCY conditions.
 */
public final class DependencyExecutor implements ConditionExecutor<Dependency> {
    private static final Logger logger = LoggerFactory.getLogger(DependencyExecutor.class);
    private static final String DEPENDENCY_INDEX_NAME = "dependency";

    // No state needed, can be stateless
    public DependencyExecutor() {}

    @Override
    // Use interface in signature
    public QueryResult execute(Dependency condition, Map<String, IndexAccessInterface> indexes,
                               Query.Granularity granularity,
                               int granularitySize,
                               String corpusName)
        throws QueryExecutionException {

        logger.debug("Executing DEPENDENCY condition: {} (corpus: {})", condition, corpusName);

        // Use interface type
        IndexAccessInterface dependencyIndex = indexes.get(DEPENDENCY_INDEX_NAME);
        if (dependencyIndex == null) {
            throw new QueryExecutionException(
                "Dependency index ('" + DEPENDENCY_INDEX_NAME + "') not found.",
                condition.toString(),
                QueryExecutionException.ErrorType.MISSING_INDEX);
        }
        
        List<MatchDetail> details = new ArrayList<>();
        String governor = condition.governor();
        String dependent = condition.dependent();
        String relation = condition.relation();
        boolean isVariable = condition.isVariable();
        String variableName = condition.variableName();
        String conditionId = String.valueOf(condition.hashCode());

        if (isVariable) {
            // Handle variable binding (iterate through index)
            details.addAll(executeVariableSearch(dependencyIndex, relation, isVariable, variableName, conditionId));
        } else if (governor != null && dependent != null && relation != null) {
            // Handle specific dependency triple
            details.addAll(executeSpecificSearch(dependencyIndex, governor, dependent, relation, isVariable, variableName, conditionId));
        } else {
            logger.warn("Unsupported DEPENDENCY condition combination: {}", condition);
            // Or throw exception? Return empty for now.
        }

        return new QueryResult(granularity, granularitySize, details);
    }

    private List<MatchDetail> executeSpecificSearch(
            IndexAccessInterface index,
            String governor,
            String dependent,
            String relation,
            boolean isVariable,
            String variableName,
            String conditionId)
        throws QueryExecutionException {
        
        try {
            // Normalize terms
            String normalizedGovernor = governor.toLowerCase();
            String normalizedDependent = dependent.toLowerCase();
            String normalizedRelation = relation.toLowerCase();
            
            // Create the search key in format "governor<DELIM>relation<DELIM>dependent"
            // Use constant from interface
            String searchKey = normalizedGovernor + IndexAccessInterface.DELIMITER + 
                               normalizedRelation + IndexAccessInterface.DELIMITER + 
                               normalizedDependent;
            byte[] keyBytes = searchKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            Optional<PositionList> positionsOpt = index.get(keyBytes);
            
            if (positionsOpt.isPresent()) {
                PositionList positionList = positionsOpt.get();
                // Use the searchKey (which includes delimiters) as the value, matching test expectations
                String value = searchKey; 
                return positionList.getPositions().stream()
                    .map(pos -> new MatchDetail(value, ValueType.DEPENDENCY, pos, conditionId, isVariable ? variableName : null))
                    .collect(Collectors.toList());
            } else {
                return Collections.emptyList();
            }
        } catch (IndexAccessException e) {
             throw new QueryExecutionException("Error accessing dependency index for specific search", e, "DEPENDENCY", QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR);
        }
    }

    private List<MatchDetail> executeVariableSearch(
            IndexAccessInterface index,
            String relation, // Relation is required for variable search prefix
            boolean isVariable,
            String variableName,
            String conditionId)
        throws QueryExecutionException {
        
        List<MatchDetail> details = new ArrayList<>();
        if (relation == null || relation.isEmpty()) {
             logger.warn("Relation is required for variable search in DEPENDENCY condition");
             return details;
        }
        
        String prefix = relation.toLowerCase() + IndexAccessInterface.DELIMITER;
        byte[] prefixBytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        logger.debug("Executing variable search with prefix: {}", prefix);
        
        try (DBIterator iterator = index.iterator()) {
            iterator.seek(prefixBytes);

            while (iterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                String key = new String(entry.getKey(), java.nio.charset.StandardCharsets.UTF_8);

                // Check if the key still starts with the prefix
                if (!key.startsWith(prefix)) {
                    break; // Moved past relevant keys
                }

                // Extract governor, relation, dependent from key
                String[] parts = key.split(String.valueOf(IndexAccessInterface.DELIMITER));
                if (parts.length == 3) {
                    String gov = parts[0];
                    String rel = parts[1]; // Should match input relation (case-insensitively)
                    String dep = parts[2];
                    
                    // Deserialize PositionList
                    PositionList positionList = PositionList.deserialize(entry.getValue());
                    String value = String.join("/", gov, rel, dep); // Reconstruct original case?
                    
                    details.addAll(positionList.getPositions().stream()
                        .map(pos -> new MatchDetail(value, ValueType.DEPENDENCY, pos, conditionId, variableName))
                        .collect(Collectors.toList()));
                } else {
                     logger.warn("Skipping invalid key format in dependency index: {}", key);
                }
            }
        } catch (Exception e) { // Catch IndexAccessException, IOException, RuntimeException from deserialize
            throw new QueryExecutionException("Error during variable search in dependency index", e, "DEPENDENCY", QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR);
        }
        
        return details;
    }
} 