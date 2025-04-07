package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.binding.BindingContext;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.TemporalPredicate;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Temporal;
import com.example.query.index.IndexManager;

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
import java.util.Optional;
import java.util.Set;

/**
 * Executor for temporal conditions in queries.
 * 
 * This class has two main responsibilities:
 * 1. For simple temporal conditions, it filters results based on date ranges using Nash or direct index scan.
 * 2. For temporal conditions with variables, it binds the matching date values to the variable.
 */
public final class TemporalExecutor implements ConditionExecutor<Temporal> {
    private static final Logger logger = LoggerFactory.getLogger(TemporalExecutor.class);
    
    private static final String DATE_INDEX = "ner_date";
    
    // Define a single date formatter for the yyyyMMdd format used in the index
    private static final DateTimeFormatter INDEX_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    // Store Nash indices per corpus
    private final Map<String, Map<String, Set<Integer>>> nashIndices = new HashMap<>();
    
    /**
     * Creates a new TemporalExecutor.
     */
    public TemporalExecutor() {
    }
    
    /**
     * Initializes the Nash index for a specific corpus using date data from the index.
     * 
     * @param corpusName The name of the corpus to initialize
     * @param indexManager The index manager for accessing indexes
     * @return true if initialization was successful, false otherwise
     */
    public boolean initializeNashIndexForCorpus(String corpusName, IndexManager indexManager) {
        if (nashIndices.containsKey(corpusName)) {
            logger.debug("Nash index already initialized for corpus: {}", corpusName);
            return true;
        }
        
        try {
            // Get the date index from the index manager
            Optional<IndexAccess> dateIndexOpt = indexManager.getIndex(DATE_INDEX);
            if (dateIndexOpt.isEmpty()) {
                logger.warn("Date index not found for corpus: {}", corpusName);
                return false;
            }
            
            // Extract date ranges from the date index
            Map<Integer, String> docDateRanges = new HashMap<>();
            Map<Integer, Set<LocalDate>> docDates = new HashMap<>();
            
            // Process all dates in the index
            try (var iterator = dateIndexOpt.get().iterator()) {
                iterator.seekToFirst();
                
                while (iterator.hasNext()) {
                    byte[] keyBytes = iterator.peekNext().getKey();
                    byte[] valueBytes = iterator.peekNext().getValue();
                    
                    // Parse date from key (yyyyMMdd format)
                    String dateStr = new String(keyBytes, StandardCharsets.UTF_8);
                    
                    try {
                        LocalDate date = LocalDate.parse(dateStr, INDEX_DATE_FORMAT);
                        
                        // Get document IDs for this date
                        PositionList positions = PositionList.deserialize(valueBytes);
                        for (Position position : positions.getPositions()) {
                            docDates.computeIfAbsent(position.getDocumentId(), k -> new HashSet<>()).add(date);
                        }
                    } catch (Exception e) {
                        logger.debug("Error processing date entry: {}", e.getMessage());
                    }
                    
                    iterator.next();
                }
            }
            
            // Create date ranges for Nash
            for (Map.Entry<Integer, Set<LocalDate>> entry : docDates.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    LocalDate minDate = entry.getValue().stream().min(LocalDate::compareTo).get();
                    LocalDate maxDate = entry.getValue().stream().max(LocalDate::compareTo).get();
                    docDateRanges.put(entry.getKey(), String.format("[%s , %s]", minDate, maxDate));
                }
            }
            
            if (docDateRanges.isEmpty()) {
                logger.warn("No date ranges found for corpus: {}", corpusName);
                return false;
            }
            
            // Build the Nash index for this corpus
            MultiMap<String, Integer> nashIdx = Nash.invert(new ArrayList<>(docDateRanges.values()));
            
            // Convert MultiMap to regular Map for storage in nashIndices
            Map<String, Set<Integer>> corpusNashIndex = new HashMap<>();
            for (String key : nashIdx.keySet()) {
                corpusNashIndex.put(key, new HashSet<>(nashIdx.get(key)));
            }
            
            // Store in corpus-specific map
            nashIndices.put(corpusName, corpusNashIndex);
            
            logger.info("Nash index initialized with {} date ranges for corpus: {}", docDateRanges.size(), corpusName);
            return true;
            
        } catch (Exception e) {
            logger.error("Failed to initialize Nash index: {}", e.getMessage(), e);
            return false;
        }
    }
    
    @Override
    public Set<DocSentenceMatch> execute(
            Temporal condition,
            Map<String, IndexAccess> indexes,
            BindingContext bindingContext,
            Query.Granularity granularity,
            int granularitySize) 
            throws QueryExecutionException {
        
        logger.debug("Executing temporal condition: {}", condition);
        
        // Validate required indexes
        if (!indexes.containsKey(DATE_INDEX)) {
            throw new QueryExecutionException(
                String.format("Missing required index: %s", DATE_INDEX),
                condition.toString(),
                QueryExecutionException.ErrorType.MISSING_INDEX
            );
        }
        
        // Get corpus name from binding context, default to "default"
        String corpus = bindingContext.getValue("corpus", String.class).orElse("default");
        
        // Check if this is a condition with a variable binding vs simple filter
        if (condition.variable().isPresent()) {
            // This is a variable binding condition - execute the condition and bind the values
            return executeTemporalWithVariableBinding(condition, corpus, indexes, bindingContext, granularity);
        } else {
            // Handle simple temporal condition without variable binding
            if (nashIndices.containsKey(corpus)) {
                return executeSimpleTemporalWithNash(condition, corpus, indexes, granularity);
            } else {
                return executeSimpleTemporalWithIndex(condition, corpus, indexes, granularity);
            }
        }
    }
    
    /**
     * Executes a simple temporal condition using Nash index.
     */
    private Set<DocSentenceMatch> executeSimpleTemporalWithNash(
            Temporal temporal,
            String corpus,
            Map<String, IndexAccess> indexes,
            Query.Granularity granularity) 
            throws QueryExecutionException {
        
        Set<Integer> matchingDocIds = new HashSet<>();
        
        Map<String, Set<Integer>> nashIndex = nashIndices.get(corpus);
        if (nashIndex == null) {
            logger.warn("Nash index not initialized for corpus: {}. Falling back to index scan.", corpus);
            // Fallback to direct index scan if Nash isn't ready for this corpus
            return executeSimpleTemporalWithIndex(temporal, corpus, indexes, granularity);
        }
        
        // Convert the temporal condition to a Nash interval
        String interval = temporal.toNashInterval();
        
        // If the interval contains only years, expand it to full ISO date format
        interval = Temporal.expandYearOnlyInterval(interval);
        logger.debug("Using Nash interval: {}", interval);
        
        // Get the Nash predicate directly from the temporal type
        Nash.RangePredicate nashPredicate = temporal.temporalType().toNashPredicate();
        
        try {
            // Generate hash prefixes for the interval with the appropriate predicate
            String[] hashPrefixes = Nash.generateTimeHash(interval, nashPredicate);
            logger.debug("Generated {} hash prefixes for interval {} using predicate {}", 
                    hashPrefixes.length, interval, nashPredicate);
            
            // Find matching document IDs from the Nash index
            for (String hashPrefix : hashPrefixes) {
                Set<Integer> docIds = nashIndex.get(hashPrefix);
                if (docIds != null) {
                    matchingDocIds.addAll(docIds);
                }
            }
            
            logger.debug("Found {} matching document IDs using Nash index", matchingDocIds.size());
            
            // Convert document IDs to result entries
            Set<DocSentenceMatch> results = new HashSet<>();
            for (Integer docId : matchingDocIds) {
                // Nash operates at document level, so use -1 for sentence ID with sentence granularity
                if (granularity == Query.Granularity.DOCUMENT) {
                    results.add(new DocSentenceMatch(docId, corpus));
                } else {
                    // Even for sentence granularity, Nash only gives doc IDs.
                    // Use -1 initially. Subsequent steps might refine sentence IDs if needed.
                    results.add(new DocSentenceMatch(docId, -1, corpus)); 
                }
            }
            
            return results;
        } catch (Exception e) {
            logger.error("Error executing Nash temporal query: {}", e.getMessage(), e);
            throw new QueryExecutionException(
                "Error in Nash temporal query: " + e.getMessage(),
                e,
                temporal.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Executes a simple temporal condition using the date index directly.
     * This is a direct index scanning approach used as an alternative to Nash when:
     * - Nash index is not initialized
     * - Sentence-level granularity with exact position information is required
     */
    private Set<DocSentenceMatch> executeSimpleTemporalWithIndex(
            Temporal temporal,
            String corpus,
            Map<String, IndexAccess> indexes,
            Query.Granularity granularity) 
            throws QueryExecutionException {
        
        logger.debug("Executing simple temporal condition with direct index scan: [{} to {}]", 
                temporal.startDate(), temporal.endDate());
        
        // Get the date index
        IndexAccess dateIndex = indexes.get(DATE_INDEX);
        Set<DocSentenceMatch> results = new HashSet<>();
        
        try {
            // Get the date range to search for
            LocalDateTime startDate = temporal.startDate();
            LocalDateTime endDate = temporal.endDate().orElse(startDate);
            
            // Iterate through the date index
            try (var iterator = dateIndex.iterator()) {
                iterator.seekToFirst();
                
                while (iterator.hasNext()) {
                    byte[] keyBytes = iterator.peekNext().getKey();
                    byte[] valueBytes = iterator.peekNext().getValue();
                    
                    // Get date value from key
                    String dateStr = new String(keyBytes, StandardCharsets.UTF_8);
                    
                    LocalDate documentDate = null;
                    
                    // Use the exact format we expect from NerDateIndexGenerator
                    try {
                        documentDate = LocalDate.parse(dateStr, INDEX_DATE_FORMAT);
                    } catch (DateTimeParseException e) {
                        iterator.next();
                        continue; // Skip keys that are not in the expected date format
                    }
                    
                    LocalDateTime documentDateTime = documentDate.atStartOfDay();
                    
                    // Check if this date satisfies the temporal condition
                    boolean matches = evaluateTemporalCondition(temporal.temporalType(), 
                                                                documentDateTime, startDate, endDate);
                    
                    if (matches) {
                        // Add all document/sentence matches for this date
                        PositionList positions = PositionList.deserialize(valueBytes);
                        for (Position position : positions.getPositions()) {
                            int docId = position.getDocumentId();
                            int sentenceId = position.getSentenceId();
                            
                            if (granularity == Query.Granularity.DOCUMENT) {
                                results.add(new DocSentenceMatch(docId, corpus));
                            } else {
                                results.add(new DocSentenceMatch(docId, sentenceId, corpus));
                            }
                        }
                    }
                    
                    iterator.next();
                }
            }
            
            logger.info("Simple temporal condition (direct index scan) matched {} results ({}-level)", 
                        results.size(), granularity);
            return results;
        } catch (Exception e) {
            throw new QueryExecutionException(
                "Error executing temporal condition via index scan: " + e.getMessage(),
                e,
                temporal.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Evaluates if a document date satisfies the temporal condition.
     * 
     * <p>This method provides a direct implementation of temporal predicates
     * and is used as an alternative to Nash when precise date matching at the
     * sentence level is needed. While Nash provides efficient indexing for
     * document-level temporal queries, this method allows for more flexible
     * matching with exact position information.</p>
     * 
     * <p>The implementation follows these semantics for each type:</p>
     * <ul>
     * <li>BEFORE: Document date is before query start date</li>
     * <li>AFTER: Document date is after query start date</li>
     * <li>BEFORE_EQUAL: Document date is before or equal to query start date</li>
     * <li>AFTER_EQUAL: Document date is after or equal to query start date</li>
     * <li>EQUAL: Document date is exactly equal to query start date</li>
     * <li>CONTAINS: Document date is within the query date range</li>
     * <li>CONTAINED_BY: Query date range is within the document date</li>
     * <li>INTERSECT: Document date overlaps with query date range</li>
     * <li>PROXIMITY: Document date is within a specified proximity of query date</li>
     * </ul>
     */
    private boolean evaluateTemporalCondition(
            TemporalPredicate type, 
            LocalDateTime docDate,
            LocalDateTime queryStart,
            LocalDateTime queryEnd) {
        
        return switch (type) {
            case BEFORE -> docDate.isBefore(queryStart);
            case AFTER -> docDate.isAfter(queryStart);
            case BEFORE_EQUAL -> docDate.isBefore(queryStart) || docDate.isEqual(queryStart);
            case AFTER_EQUAL -> docDate.isAfter(queryStart) || docDate.isEqual(queryStart);
            case EQUAL -> docDate.isEqual(queryStart);
            case CONTAINS -> (docDate.isEqual(queryStart) || docDate.isAfter(queryStart)) && 
                             (docDate.isEqual(queryEnd) || docDate.isBefore(queryEnd));
            case CONTAINED_BY -> (queryStart.isEqual(docDate) || queryStart.isAfter(docDate)) && 
                                 (queryEnd.isEqual(docDate) || queryEnd.isBefore(docDate));
            case INTERSECT -> !(docDate.isBefore(queryStart) || docDate.isAfter(queryEnd));
            case PROXIMITY -> {
                // For PROXIMITY, check if within a certain distance (default 1 day)
                long days = Math.abs(java.time.Duration.between(docDate, queryStart).toDays());
                yield days <= 1;
            }
        };
    }
    
    /**
     * Represents a matched date with position information.
     * This is similar to NerExecutor.MatchedEntityValue.
     * TODO: Consider moving this to a shared location if used elsewhere.
     */
    public record MatchedDateValue(LocalDate date, int beginPosition, int endPosition, int documentId, int sentenceId) {
        @Override
        public String toString() {
            return String.format("%s@%d:%d (Doc: %d, Sent: %d)", date, beginPosition, endPosition, documentId, sentenceId);
        }
    }

    /**
     * Executes a temporal condition with variable binding.
     * This handles the case where date values should be extracted and bound to a variable.
     */
    private Set<DocSentenceMatch> executeTemporalWithVariableBinding(
            Temporal condition,
            String corpus,
            Map<String, IndexAccess> indexes,
            BindingContext bindingContext,
            Query.Granularity granularity) 
            throws QueryExecutionException {
        
        logger.debug("Executing temporal condition with variable binding: {}", condition.variable().get());
        
        // Get the variable name
        String variableName = condition.variable().get();
        
        // Validate variable name format (simple name, no dots)
        if (variableName.contains(".")) {
             throw new QueryExecutionException(
                String.format("Invalid variable name format for binding: '%s'. Should not contain '.'", variableName),
                condition.toString(),
                QueryExecutionException.ErrorType.INVALID_CONDITION
            );
        }
        
        // First, get matching documents/sentences using standard temporal filtering
        // IMPORTANT: We MUST use the direct index scan method here to get sentence IDs if needed.
        // Nash only provides document IDs.
        Set<DocSentenceMatch> baseMatches = executeSimpleTemporalWithIndex(condition, corpus, indexes, granularity);
        
        // If no base matches, return early
        if (baseMatches.isEmpty()) {
            logger.debug("No base matches found for temporal variable binding.");
            return baseMatches;
        }
        
        logger.debug("Found {} base matches for variable binding.", baseMatches.size());

        // Now bind the extracted date values to the variable for each match
        IndexAccess dateIndex = indexes.get(DATE_INDEX);
        LocalDateTime startDate = condition.startDate();
        LocalDateTime endDate = condition.endDate().orElse(startDate);
        
        Set<DocSentenceMatch> finalMatches = new HashSet<>(); // Collect matches that successfully bind a value
        
        try {
            // Use a map to store found dates per document/sentence to avoid redundant lookups
            Map<Integer, Map<Integer, List<MatchedDateValue>>> foundDates = new HashMap<>(); 

            // Iterate through all date index entries ONCE to find all relevant dates
            try (var iterator = dateIndex.iterator()) {
                iterator.seekToFirst();
                
                while (iterator.hasNext()) {
                    byte[] keyBytes = iterator.peekNext().getKey();
                    byte[] valueBytes = iterator.peekNext().getValue();
                    
                    String dateStr = new String(keyBytes, StandardCharsets.UTF_8);
                    LocalDate documentDate;
                    try {
                        documentDate = LocalDate.parse(dateStr, INDEX_DATE_FORMAT);
                    } catch (DateTimeParseException e) {
                        iterator.next();
                        continue; // Skip non-date keys
                    }
                    
                    LocalDateTime documentDateTime = documentDate.atStartOfDay();
                    
                    // Check if this date satisfies the temporal condition
                    if (evaluateTemporalCondition(condition.temporalType(), 
                                                documentDateTime, startDate, endDate)) {
                        
                        // Get positions for this date
                        PositionList positions = PositionList.deserialize(valueBytes);
                        for (Position position : positions.getPositions()) {
                            int docId = position.getDocumentId();
                            int sentenceId = position.getSentenceId();
                            
                            // Store this found date, associated with its doc/sentence
                            foundDates.computeIfAbsent(docId, k -> new HashMap<>())
                                      .computeIfAbsent(sentenceId, k -> new ArrayList<>())
                                      .add(new MatchedDateValue(
                                            documentDate, 
                                            position.getBeginPosition(), 
                                            position.getEndPosition(),
                                            docId, sentenceId));
                        }
                    }
                    iterator.next();
                }
            }

            // Now, iterate through the base matches and bind the found dates
            for (DocSentenceMatch match : baseMatches) {
                int docId = match.documentId();
                int sentenceId = match.sentenceId(); // Will be -1 for document granularity

                List<MatchedDateValue> datesToBind = new ArrayList<>();
                Map<Integer, List<MatchedDateValue>> docFoundDates = foundDates.get(docId);

                if (docFoundDates != null) {
                    if (granularity == Query.Granularity.DOCUMENT) {
                        // For document level, collect all dates found in any sentence of that doc
                        docFoundDates.values().forEach(datesToBind::addAll);
                    } else {
                        // For sentence level, get dates only for that specific sentence
                         List<MatchedDateValue> sentenceDates = docFoundDates.get(sentenceId);
                         if (sentenceDates != null) {
                            datesToBind.addAll(sentenceDates);
                         }
                    }
                }
                
                // If we found dates for this match, bind them and add to final results
                if (!datesToBind.isEmpty()) {
                    // Bind all found date values associated with this match
                    // Note: BindingContext might need adjustment if multiple values are bound to the same variable name.
                    // Currently, it likely overwrites. Let's bind the list for now.
                    // TODO: Clarify BindingContext behavior with multiple values.
                    
                    // Using the first found date for simplicity now.
                    MatchedDateValue firstDate = datesToBind.get(0);
                    
                    // Convert MatchedDateValue to NerExecutor.MatchedEntityValue for binding
                    // TODO: Create a common value type?
                     NerExecutor.MatchedEntityValue bindValue = new NerExecutor.MatchedEntityValue(
                        firstDate.date().format(INDEX_DATE_FORMAT), // Bind using the index format string
                        firstDate.beginPosition(), 
                        firstDate.endPosition(),
                        firstDate.documentId(), 
                        firstDate.sentenceId()
                    );

                    bindingContext.bindValue(variableName, bindValue); 
                    match.setVariableValue(variableName, bindValue); 
                    finalMatches.add(match); // Add the match with the bound value

                    if (datesToBind.size() > 1) {
                         logger.warn("Multiple ({}) date values found for variable '{}' at doc:{}, sent:{}. Binding only the first: {}", 
                                    datesToBind.size(), variableName, docId, sentenceId, firstDate);
                    } else {
                        logger.debug("Bound DATE value '{}' at doc:{}, sent:{} to variable '{}'", 
                                   firstDate, docId, sentenceId, variableName);
                    }
                } else {
                     logger.debug("No specific date value found to bind for match: {}", match);
                }
            }

        } catch (Exception e) {
            logger.error("Error binding date values: {}", e.getMessage(), e);
            throw new QueryExecutionException(
                "Error binding date values: " + e.getMessage(),
                e,
                condition.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
        
        logger.debug("Temporal variable binding complete. Returning {} final matches.", finalMatches.size());
        return finalMatches;
    }
} 