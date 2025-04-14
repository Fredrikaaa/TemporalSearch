package com.example.query.executor;

import com.example.core.IndexAccessInterface;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.binding.MatchDetail;
import com.example.query.binding.ValueType;
import com.example.query.model.Query;
import com.example.query.model.TemporalPredicate;
import com.example.query.model.condition.Temporal;
import com.example.query.index.IndexManager;
import com.example.query.executor.QueryResult;

import org.apache.pig.impl.util.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.ntnu.sandbox.Nash;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.nio.file.Path;
import java.util.stream.Collectors;

/**
 * Executor for temporal conditions in queries.
 * Returns QueryResult containing MatchDetail objects.
 */
public final class TemporalExecutor implements ConditionExecutor<Temporal> {
    private static final Logger logger = LoggerFactory.getLogger(TemporalExecutor.class);
    
    private static final String DATE_INDEX = "ner_date";
    
    // Formatter for parsing keys from the ner_date index (YYYYMMDD)
    private static final DateTimeFormatter INDEX_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    // Formatter for creating interval strings for Nash.invert ([YYYY-MM-DD , YYYY-MM-DD])
    private static final DateTimeFormatter NASH_INTERVAL_FORMATTER = DateTimeFormatter.ISO_DATE;
    
    // Store Nash indices per corpus: Map<CorpusName, Map<NashHashPrefix, Set<DocID>>>
    private final Map<String, Map<String, Set<Integer>>> nashIndices = new HashMap<>();
    
    /**
     * Creates a new TemporalExecutor.
     */
    public TemporalExecutor() {
    }
    
    /**
     * Initializes the Nash index structure for a specific corpus by reading from the DATE_INDEX.
     * It iterates through the DATE_INDEX, creates interval strings, generates Nash hashes,
     * and stores a mapping from Nash hash prefixes to the document IDs associated with those intervals.
     */
    public boolean initializeNashIndexForCorpus(String corpusName, IndexManager indexManager) {
        if (nashIndices.containsKey(corpusName)) {
            logger.debug("Nash index already initialized for corpus: {}", corpusName);
            return true;
        }

        logger.info("Initializing Nash index for corpus: {}", corpusName);

        Optional<IndexAccessInterface> indexOpt = indexManager.getIndex(DATE_INDEX);
        if (indexOpt.isEmpty()) {
            logger.error("Cannot initialize Nash index: '{}' index not found via IndexManager for corpus '{}'.", DATE_INDEX, corpusName);
            return false;
        }
        IndexAccessInterface dateIndex = indexOpt.get();

        // Prepare data for Nash.invert
        List<String> intervalStrings = new ArrayList<>();
        // Temporary map to link list index back to document IDs
        Map<Integer, Set<Integer>> listIndexToDocIds = new HashMap<>();
        int intervalIndex = 0;

        try (var iterator = dateIndex.iterator()) {
            iterator.seekToFirst();
            while (iterator.hasNext()) {
                Entry<byte[], byte[]> entry = iterator.next(); // Use next() to get and advance
                String dateStr = new String(entry.getKey(), StandardCharsets.UTF_8);
                LocalDate docDate = parseDateKey(dateStr); // Reuse existing parsing logic

                if (docDate != null) {
                    // Format as "[YYYY-MM-DD , YYYY-MM-DD]" for Nash
                    String interval = String.format("[%s , %s]",
                            NASH_INTERVAL_FORMATTER.format(docDate),
                            NASH_INTERVAL_FORMATTER.format(docDate));
                    intervalStrings.add(interval);

                    // Extract document IDs from the value (PositionList)
                    PositionList positions = PositionList.deserialize(entry.getValue());
                    Set<Integer> docIds = positions.getPositions().stream()
                                                    .map(Position::getDocumentId)
                                                    .collect(Collectors.toSet());

                    listIndexToDocIds.put(intervalIndex, docIds);
                    intervalIndex++;
                } else {
                    logger.warn("Skipping invalid date key during Nash initialization: {}", dateStr);
                }
            }
        } catch (Exception e) {
            logger.error("Error reading from '{}' index during Nash initialization for corpus '{}': {}", DATE_INDEX, corpusName, e.getMessage(), e);
            return false;
        }

        logger.debug("Prepared {} interval strings from '{}' index for Nash inversion.", intervalStrings.size(), DATE_INDEX);

        if (intervalStrings.isEmpty()) {
            logger.warn("No valid date intervals found in '{}' index for corpus '{}'. Nash index will be empty.", DATE_INDEX, corpusName);
            // Initialize with an empty map to avoid checks failing later
            nashIndices.put(corpusName, Collections.emptyMap());
            return true; // Technically initialized, just empty.
        }

        try {
            // Generate Nash structure: MultiMap<NashHashPrefix, IntervalListIndex>
            MultiMap<String, Integer> invertedIndex = Nash.invert(intervalStrings);

            // Convert to final structure: Map<NashHashPrefix, Set<DocID>>
            Map<String, Set<Integer>> corpusNashIndex = new HashMap<>();
            for (String nashPrefix : invertedIndex.keySet()) {
                Set<Integer> docIdSet = new HashSet<>();
                // For each prefix, get the list indices it maps to
                for (Integer listIdx : invertedIndex.get(nashPrefix)) {
                    // Use the temporary map to find the actual doc IDs for that list index
                    Set<Integer> ids = listIndexToDocIds.get(listIdx);
                    if (ids != null) {
                        docIdSet.addAll(ids);
                    } else {
                         logger.warn("Inconsistency: Nash prefix '{}' mapped to list index {} which has no associated doc IDs.", nashPrefix, listIdx);
                    }
                }
                 if (!docIdSet.isEmpty()) {
                    corpusNashIndex.put(nashPrefix, docIdSet);
                 }
            }

            // Store the final index for the corpus
            nashIndices.put(corpusName, corpusNashIndex);

            // Log the number of unique prefixes, not date ranges
            logger.info("Nash index initialized with {} unique hash prefixes for corpus: {}", corpusNashIndex.size(), corpusName);
            return true;

        } catch (Exception e) { // Catch potential exceptions from Nash.invert
            logger.error("Failed to generate Nash index structure for corpus '{}': {}", corpusName, e.getMessage(), e);
            return false;
        }
    }
    
    @Override
    public QueryResult execute(
            Temporal condition,
            Map<String, IndexAccessInterface> indexes,
            Query.Granularity granularity,
            int granularitySize,
            String corpusName)
            throws QueryExecutionException {
        
        logger.debug("Executing temporal condition: {} for corpus: {}", condition, corpusName);
        
        if (!indexes.containsKey(DATE_INDEX)) {
            throw new QueryExecutionException(String.format("Missing required index: %s", DATE_INDEX), condition.toString(), QueryExecutionException.ErrorType.MISSING_INDEX);
        }
        
        List<MatchDetail> details;
        boolean isVariable = condition.variable().isPresent();

        try {
            if (isVariable) {
                // Variable binding always needs direct scan
                logger.debug("Temporal condition has variable '{}', using variable extraction path.", condition.variable().get());
                details = executeTemporalVariableExtraction(condition, indexes);
            } else if (granularity == Query.Granularity.SENTENCE) {
                // Sentence granularity requires precise positions, bypass Nash
                logger.debug("Sentence granularity requested, using direct index scan path.");
                details = executeSimpleTemporalWithIndex(condition, indexes); // Use direct scan
            } else {
                // Document granularity or default: prefer Nash if available
                if (nashIndices.containsKey(corpusName)) {
                    logger.debug("Document granularity: Using Nash index path.");
                    details = executeSimpleTemporalWithNash(condition, corpusName); // Use Nash
                } else {
                    logger.warn("Nash index not available for corpus '{}', falling back to index scan for temporal condition.", corpusName);
                    logger.debug("Document granularity: Nash index unavailable, using direct index scan path.");
                    details = executeSimpleTemporalWithIndex(condition, indexes); // Fallback to direct scan
                }
            }

            logger.debug("Temporal condition produced {} MatchDetail objects. Returning QueryResult.", details.size());

            // Create QueryResult directly
            QueryResult finalResult = new QueryResult(granularity, granularitySize, details);

            logger.debug("Temporal execution complete with {} MatchDetail objects.", finalResult.getAllDetails().size());
            return finalResult;

        } catch (Exception e) {
            if (e instanceof QueryExecutionException qee) { throw qee; }
            throw new QueryExecutionException("Error executing temporal condition: " + e.getMessage(), e, condition.toString(), QueryExecutionException.ErrorType.INTERNAL_ERROR);
        }
    }
    
    /**
     * Executes a simple temporal condition using Nash index, returning MatchDetail list.
     */
    private List<MatchDetail> executeSimpleTemporalWithNash(
            Temporal condition,
            String corpus)
            throws QueryExecutionException {
        
        Map<String, Set<Integer>> nashIndex = nashIndices.get(corpus);
        if (nashIndex == null) {
            logger.error("Nash index unexpectedly null for corpus: {}. Returning empty list.", corpus);
            return Collections.emptyList(); 
        }
        
        List<MatchDetail> details = new ArrayList<>();
        String conditionId = String.valueOf(condition.hashCode());
        String interval = Temporal.expandYearOnlyInterval(condition.toNashInterval());
        Nash.RangePredicate nashPredicate = condition.temporalType().toNashPredicate();
        logger.debug("Querying Nash index for corpus '{}' with interval: {}, predicate: {}", corpus, interval, nashPredicate);

        try {
            String[] hashPrefixes = Nash.generateTimeHash(interval, nashPredicate);
            Set<Integer> matchingDocIds = new HashSet<>();
            for (String hashPrefix : hashPrefixes) {
                 Set<Integer> docIds = nashIndex.get(hashPrefix);
                 if (docIds != null) {
                     matchingDocIds.addAll(docIds);
                 }
            }
            
            logger.debug("Nash query found {} matching document IDs", matchingDocIds.size());
            
            // Create placeholder MatchDetail for each doc ID
            for (Integer docId : matchingDocIds) {
                 Position placeholderPos = new Position(docId, -1, -1, -1, null);
                 // Use interval string as placeholder value? Yes, for consistency
                 details.add(new MatchDetail(interval, ValueType.DATE, placeholderPos, conditionId, null));
            }
            return details;
        } catch (Exception e) {
            throw new QueryExecutionException("Error querying Nash index: " + e.getMessage(), e, condition.toString(), QueryExecutionException.ErrorType.INTERNAL_ERROR);
        }
    }
    
    /**
     * Executes a simple temporal condition using the date index directly, returning MatchDetail list.
     */
    private List<MatchDetail> executeSimpleTemporalWithIndex(
            Temporal condition,
            Map<String, IndexAccessInterface> indexes)
            throws QueryExecutionException {
        
        List<MatchDetail> details = new ArrayList<>();
        String conditionId = String.valueOf(condition.hashCode());
        IndexAccessInterface dateIndex = indexes.get(DATE_INDEX);
        TemporalPredicate type = condition.temporalType();
        LocalDateTime queryStart = condition.startDate();
        LocalDateTime queryEnd = condition.endDate().orElse(queryStart);

        logger.debug("Scanning DATE index directly for condition: {} ({} to {})", type, queryStart, queryEnd);

        try (var iterator = dateIndex.iterator()) {
            iterator.seekToFirst(); // Start scan from the beginning
            while (iterator.hasNext()) {
                 Entry<byte[], byte[]> currentEntry = iterator.peekNext();
                 String dateStr = new String(currentEntry.getKey(), StandardCharsets.UTF_8);
                 LocalDate docDate = parseDateKey(dateStr);
                
                 iterator.next(); // Consume entry
                
                 if (docDate != null) {
                     // Evaluate the condition using helper method
                     if (evaluateTemporalCondition(type, docDate.atStartOfDay(), queryStart, queryEnd)) {
                         PositionList positions = PositionList.deserialize(currentEntry.getValue());
                         for (Position position : positions.getPositions()) {
                             // Create MatchDetail for each position matching the date criteria
                             details.add(new MatchDetail(docDate, ValueType.DATE, position, conditionId, null));
                         }
                     }
                 }
            }
        } catch (Exception e) {
             throw new QueryExecutionException("Error scanning DATE index: " + e.getMessage(), e, condition.toString(), QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR);
        }
        logger.debug("DATE index scan found {} matching details", details.size());
        return details;
    }
    
    /**
     * Executes temporal variable extraction using the date index, returning MatchDetail list.
     */
    private List<MatchDetail> executeTemporalVariableExtraction(
            Temporal condition,
            Map<String, IndexAccessInterface> indexes)
            throws QueryExecutionException {
        
        List<MatchDetail> details = new ArrayList<>();
        String conditionId = String.valueOf(condition.hashCode());
        String variableName = condition.variable().orElse(null);
        if (variableName == null) {
             logger.warn("Variable name missing in temporal variable extraction mode. Condition: {}", condition);
             return details;
        }

        IndexAccessInterface dateIndex = indexes.get(DATE_INDEX);
        TemporalPredicate type = condition.temporalType();
        LocalDateTime queryStart = condition.startDate();
        LocalDateTime queryEnd = condition.endDate().orElse(queryStart);
        
        logger.debug("Scanning DATE index for variable '{}' extraction: {} ({} to {})", variableName, type, queryStart, queryEnd);

        try (var iterator = dateIndex.iterator()) {
             iterator.seekToFirst();
             while (iterator.hasNext()) {
                 Entry<byte[], byte[]> currentEntry = iterator.peekNext();
                 String dateStr = new String(currentEntry.getKey(), StandardCharsets.UTF_8);
                 LocalDate docDate = parseDateKey(dateStr);
                
                 iterator.next(); // Consume entry
                
                 if (docDate != null) {
                     if (evaluateTemporalCondition(type, docDate.atStartOfDay(), queryStart, queryEnd)) {
                         PositionList positions = PositionList.deserialize(currentEntry.getValue());
                         for (Position position : positions.getPositions()) {
                             details.add(new MatchDetail(docDate, ValueType.DATE, position, conditionId, variableName));
                         }
                     }
                 }
             }
        } catch (Exception e) {
             throw new QueryExecutionException("Error scanning DATE index for variable extraction: " + e.getMessage(), e, condition.toString(), QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR);
        }
        logger.debug("DATE index variable extraction found {} details for '{}'", details.size(), variableName);
        return details;
    }
    
    /**
     * Parses a date string from the index key.
     * Expects format like 'YYYY-MM-DD'. Returns null if parsing fails.
     */
    private LocalDate parseDateKey(String dateStr) {
        try {
            // Trim potential whitespace before parsing
            return LocalDate.parse(dateStr.trim(), INDEX_DATE_FORMATTER);
        } catch (DateTimeParseException e) {
            logger.trace("Failed to parse date key '{}': {}", dateStr, e.getMessage()); // Trace level might be better
            return null;
        }
    }
    
    /**
     * Evaluates if a document date satisfies the temporal condition.
     * @param type The TemporalPredicate (CONTAINS, INTERSECT, etc.)
     * @param docDate The start date associated with the document/entry.
     * @param queryStart The start of the query interval.
     * @param queryEnd The end of the query interval.
     * @return true if the condition is met, false otherwise.
     */
     private boolean evaluateTemporalCondition(TemporalPredicate type, LocalDateTime docDateTime, LocalDateTime queryStart, LocalDateTime queryEnd) {
         // Assuming ner_date only stores single dates, not ranges.
         // So docStart = docEnd = docDateTime
         LocalDateTime docStart = docDateTime;
         LocalDateTime docEnd = docDateTime;

         return switch (type) {
             case CONTAINS -> // Query interval [queryStart, queryEnd] must contain doc interval [docStart, docEnd]
                 !queryStart.isAfter(docStart) && !queryEnd.isBefore(docEnd);
             case CONTAINED_BY -> // Doc interval [docStart, docEnd] must contain query interval [queryStart, queryEnd]
                 !docStart.isAfter(queryStart) && !docEnd.isBefore(queryEnd);
             case INTERSECT -> // Intervals overlap
                 !queryStart.isAfter(docEnd) && !queryEnd.isBefore(docStart);
             // Add other cases like BEFORE, AFTER, EQUALS if needed
             default -> {
                 logger.warn("Unsupported TemporalPredicate type: {}", type);
                 yield false;
             }
         };
     }
} 