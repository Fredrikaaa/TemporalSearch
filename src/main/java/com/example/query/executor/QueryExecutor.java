package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Logical.LogicalOperator;
import com.example.query.model.JoinCondition;
import com.example.query.model.Query;
import com.example.query.model.SubquerySpec;
import com.example.query.binding.BindingContext;
import com.example.query.result.ResultGenerationException;
import com.example.query.result.TableResultService;
import com.example.query.sqlite.SqliteAccessor;
import com.example.query.index.IndexManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import tech.tablesaw.api.Table;

/**
 * Executes queries against the provided indexes.
 * Responsible for coordinating the execution of all conditions in a query
 * and combining their results according to the query's logical structure.
 * 
 * Supports the execution of subqueries and joins between result sets.
 */
public class QueryExecutor {
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
    
    private final ConditionExecutorFactory executorFactory;
    private BindingContext bindingContext;
    private TableResultService tableResultService;
    private JoinExecutor joinExecutor;
    private boolean nashInitialized = false;
    
    /**
     * Creates a new QueryExecutor with the provided executor factory.
     *
     * @param executorFactory Factory for creating condition executors
     */
    public QueryExecutor(ConditionExecutorFactory executorFactory) {
        this(executorFactory, new JoinExecutor(), new TableResultService());
    }

    /**
     * Constructor for testing purposes, allowing injection of mocks.
     *
     * @param executorFactory Factory for creating condition executors
     * @param joinExecutor Mocked JoinExecutor
     * @param tableResultService Mocked TableResultService
     */
    QueryExecutor(ConditionExecutorFactory executorFactory, JoinExecutor joinExecutor, TableResultService tableResultService) {
        this.executorFactory = executorFactory;
        this.bindingContext = BindingContext.empty();
        this.joinExecutor = joinExecutor;
        this.tableResultService = tableResultService;
    }
    
    /**
     * Initializes the Nash temporal index for a specific corpus.
     * This should be called before executing queries with temporal conditions.
     * 
     * @param corpusName The corpus/source name to initialize Nash index for
     * @param indexManager The index manager for accessing indexes
     */
    public void initializeNashIndex(String corpusName, com.example.query.index.IndexManager indexManager) {
        if (nashInitialized) {
            logger.debug("Nash index already initialized, skipping");
            return;
        }
        
        try {
            // Get the TemporalExecutor from the factory
            TemporalExecutor temporalExecutor = (TemporalExecutor) executorFactory.getExecutor(
                    new com.example.query.model.condition.Temporal(
                            com.example.query.model.TemporalPredicate.EQUAL, 
                            java.time.LocalDateTime.now()));
            
            // Let the TemporalExecutor handle the Nash initialization with the index manager
            boolean success = temporalExecutor.initializeNashIndexForCorpus(corpusName, indexManager);
            if (success) {
                nashInitialized = true;
                logger.info("Nash index successfully initialized for corpus: {}", corpusName);
            } else {
                logger.warn("Failed to initialize Nash index for corpus: {}", corpusName);
            }
        } catch (Exception e) {
            logger.error("Failed to initialize Nash index: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Executes a query against the provided indexes
     *
     * @param query The query to execute
     * @param indexes Map of index name to IndexAccess
     * @return Set of matches (document or sentence level based on query granularity)
     * @throws QueryExecutionException if execution fails
     */
    public Set<DocSentenceMatch> execute(Query query, Map<String, IndexAccess> indexes) 
            throws QueryExecutionException {
        // We don't initialize Nash here anymore - it should be done before execution
        
        // Reset binding context for this execution
        this.bindingContext = BindingContext.empty();
        
        return executeWithContext(query, indexes, new SubqueryContext());
    }
    
    /**
     * Executes a query with an existing subquery context.
     * This allows for recursive execution of subqueries.
     *
     * @param query The query to execute
     * @param indexes Map of index name to IndexAccess
     * @param subqueryContext Context containing results of previously executed subqueries
     * @return Set of matches
     * @throws QueryExecutionException if execution fails
     */
    public Set<DocSentenceMatch> executeWithContext(Query query, Map<String, IndexAccess> indexes, SubqueryContext subqueryContext) 
            throws QueryExecutionException {
        logger.debug("Executing query: {}", query);
        
        // Create a fresh binding context for this execution
        this.bindingContext = BindingContext.empty();
        
        // Set granularity for execution
        Query.Granularity granularity = query.granularity();
        int granularitySize = query.granularitySize().orElse(1);
        
        // Validate granularity size
        if (granularitySize < 0 || granularitySize > 10) {
            throw new IllegalArgumentException("Granularity size must be between 0 and 10, got: " + granularitySize);
        }
        
        // Get the source name from the query - needed early for join handler
        String source = query.source();
        logger.debug("Using source: {}", source);

        // Execute any subqueries first, as they are needed for joins
        if (query.hasSubqueries()) {
            logger.debug("Executing {} subqueries", query.subqueries().size());
            executeSubqueries(query.subqueries(), indexes, subqueryContext);
        }

        // Check if this query involves a JOIN operation
        if (query.joinCondition().isPresent()) {
            logger.debug("Query has a JOIN condition. Delegating to JoinHandler.");
            // Phase 3: Instantiate and call JoinHandler here
            JoinHandler joinHandler = new JoinHandler(joinExecutor, tableResultService);
            return joinHandler.handleJoin(query, indexes, subqueryContext, source); 
        }

        // --- Execution path for queries WITHOUT a JOIN clause ---
        
        // Get all conditions from the query (excluding the join condition itself)
        List<Condition> conditions = query.conditions();
        
        if (conditions.isEmpty()) {
            // If there are no conditions AND no join, result is empty
            logger.debug("Query has no conditions and no join, returning empty result set");
            return new HashSet<>();
        }
        
        // Optimize execution order based on variable dependencies
        List<Condition> orderedConditions = optimizeExecutionOrder(conditions);
        
        Set<DocSentenceMatch> results;
        if (orderedConditions.size() == 1) {
            results = executeCondition(orderedConditions.get(0), indexes, granularity, granularitySize);
        } else {
            // Execute conditions in optimized order
            results = executeConditionsSequentially(orderedConditions, indexes, granularity, granularitySize);
        }
        
        results = ensureSourceSet(results, source);

        // Remove the applyJoin calls here, as joins are handled above
        /*
        if (query.joinCondition().isPresent() && !query.subqueries().isEmpty()) {
             try {
                 results = applyJoin(results, query, indexes, subqueryContext);
             } catch (ResultGenerationException e) {
                 throw new QueryExecutionException("Failed to apply join: " + e.getMessage(), e, 
                         "join", QueryExecutionException.ErrorType.INTERNAL_ERROR);
             }
        }
        */
        
        return results;
    }
    
    /**
     * Executes all subqueries and adds their results to the subquery context.
     *
     * @param subqueries List of subquery specifications
     * @param indexes Map of index name to IndexAccess
     * @param subqueryContext Context to store subquery results
     * @throws QueryExecutionException if execution fails
     */
    private void executeSubqueries(List<SubquerySpec> subqueries, Map<String, IndexAccess> indexes, SubqueryContext subqueryContext) 
            throws QueryExecutionException {
        for (SubquerySpec subquery : subqueries) {
            // Skip if already executed
            if (subqueryContext.hasResults(subquery.alias())) {
                logger.debug("Subquery with alias '{}' already executed, skipping", subquery.alias());
                continue;
            }
            
            logger.debug("Executing subquery: {}", subquery);
            
            // Execute the subquery recursively
            Set<DocSentenceMatch> subqueryResults = executeWithContext(subquery.subquery(), indexes, subqueryContext);
            
            // Add native results to context
            subqueryContext.addNativeResults(subquery, subqueryResults);
            
            try {
                // Convert to table using TableResultService
                Table subqueryTable = tableResultService.generateTable(
                        subquery.subquery(), 
                        subqueryResults, 
                        BindingContext.empty(), // Each subquery has its own binding context
                        indexes
                );
                
                // Add table results to context
                subqueryContext.addTableResults(subquery, subqueryTable);
                
                logger.debug("Subquery '{}' executed, generated table with {} rows", 
                        subquery.alias(), subqueryTable.rowCount());
            } catch (ResultGenerationException e) {
                throw new QueryExecutionException("Failed to generate table for subquery: " + e.getMessage(), e, 
                        "subquery", QueryExecutionException.ErrorType.INTERNAL_ERROR);
            }
        }
    }
    
    /**
     * Optimizes the execution order of conditions based on variable dependencies.
     * Conditions that produce variables should be executed before conditions that consume them.
     *
     * @param conditions The original list of conditions
     * @return A new list with optimized execution order
     */
    private List<Condition> optimizeExecutionOrder(List<Condition> conditions) {
        if (conditions.size() <= 1) {
            return conditions;
        }
        
        // Create a copy to manipulate
        List<Condition> remaining = new ArrayList<>(conditions);
        List<Condition> ordered = new ArrayList<>();
        Set<String> availableVariables = new HashSet<>();
        
        // Continue until all conditions are ordered
        while (!remaining.isEmpty()) {
            boolean progress = false;
            
            // Find conditions that can be executed with available variables
            for (int i = 0; i < remaining.size(); i++) {
                Condition condition = remaining.get(i);
                
                // Check if all consumed variables are available
                boolean canExecute = true;
                for (String var : condition.getConsumedVariables()) {
                    if (!availableVariables.contains(var)) {
                        canExecute = false;
                        break;
                    }
                }
                
                if (canExecute) {
                    // Add condition to ordered list
                    ordered.add(condition);
                    remaining.remove(i);
                    
                    // Add produced variables to available set
                    availableVariables.addAll(condition.getProducedVariables());
                    
                    progress = true;
                    break;
                }
            }
            
            // If no progress was made, we have a circular dependency
            // In this case, add the next condition and continue
            if (!progress && !remaining.isEmpty()) {
                Condition next = remaining.remove(0);
                ordered.add(next);
                availableVariables.addAll(next.getProducedVariables());
            }
        }
        
        logger.debug("Optimized execution order: {}", ordered.stream().map(Condition::getType).collect(Collectors.toList()));
        return ordered;
    }
    
    /**
     * Executes conditions sequentially in the given order.
     * Each condition execution updates the binding context for subsequent conditions.
     *
     * @param conditions The conditions to execute in order
     * @param indexes Map of index name to IndexAccess
     * @param granularity The query granularity
     * @param granularitySize The window size for sentence granularity
     * @return Combined results of all condition executions
     * @throws QueryExecutionException if execution fails
     */
    private Set<DocSentenceMatch> executeConditionsSequentially(
            List<Condition> conditions,
            Map<String, IndexAccess> indexes,
            Query.Granularity granularity,
            int granularitySize) 
            throws QueryExecutionException {
        
        Set<DocSentenceMatch> results = new HashSet<>();
        
        for (Condition condition : conditions) {
            // Prepare binding context for this condition
            bindingContext = condition.prepareBindingContext(bindingContext);
            
            // Execute the condition
            Set<DocSentenceMatch> conditionResults = executeCondition(condition, indexes, granularity, granularitySize);
            
            // For the first condition, use its results directly
            if (results.isEmpty()) {
                results = conditionResults;
            } else {
                // For subsequent conditions, intersect with previous results
                results.retainAll(conditionResults);
            }
            
            // If no results left, we can stop early
            if (results.isEmpty()) {
                break;
            }
        }
        
        return results;
    }
    
    /**
     * Ensures that all matches have the correct source set.
     * 
     * @param matches The matches to process
     * @param source The source to set
     * @return A new set of matches with the source set
     */
    private Set<DocSentenceMatch> ensureSourceSet(Set<DocSentenceMatch> matches, String source) {
        Set<DocSentenceMatch> result = new HashSet<>();
        
        for (DocSentenceMatch match : matches) {
            // If the source is already set correctly, use the match as is
            if (source.equals(match.getSource())) {
                result.add(match);
                continue;
            }
            
            // Otherwise, create a new match with the correct source
            DocSentenceMatch newMatch;
            if (match.isSentenceLevel()) {
                newMatch = new DocSentenceMatch(
                    match.documentId(), 
                    match.sentenceId(), 
                    match.getAllPositions(), 
                    source,
                    match.getVariableValues()
                );
            } else {
                newMatch = new DocSentenceMatch(
                    match.documentId(), 
                    -1,  // Document-level match
                    match.getAllPositions(), 
                    source,
                    match.getVariableValues()
                );
            }
            
            result.add(newMatch);
        }
        
        return result;
    }
    
    /**
     * Executes a single condition against the indexes.
     *
     * @param condition The condition to execute
     * @param indexes Map of index name to IndexAccess
     * @param granularity The query granularity
     * @param granularitySize The window size for sentence granularity
     * @return Set of matches at the specified granularity level
     * @throws QueryExecutionException if execution fails
     */
    @SuppressWarnings("unchecked")
    private Set<DocSentenceMatch> executeCondition(
            Condition condition,
            Map<String, IndexAccess> indexes,
            Query.Granularity granularity,
            int granularitySize) 
            throws QueryExecutionException {
        logger.debug("Executing condition: {} with granularity: {} and size: {}", 
                condition, granularity, granularitySize);
        
        try {
            // Get the appropriate executor for this condition type
            ConditionExecutor executor = executorFactory.getExecutor(condition);
            
            // Execute the condition with the current binding context
            return executor.execute(condition, indexes, bindingContext, granularity, granularitySize);
        } catch (QueryExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryExecutionException(
                "Error executing condition: " + e.getMessage(),
                e,
                condition.toString(),
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Gets the current binding context.
     *
     * @return The current binding context
     */
    public BindingContext getBindingContext() {
        return bindingContext;
    }
    
    /**
     * Extracts document IDs from a set of matches.
     *
     * @param matches The matches to process
     * @return Set of document IDs
     */
    public Set<Integer> getDocumentIds(Set<DocSentenceMatch> matches) {
        return matches.stream()
            .map(DocSentenceMatch::documentId)
            .collect(Collectors.toSet());
    }
} 