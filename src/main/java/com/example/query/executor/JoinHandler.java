package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.query.binding.BindingContext; // May be needed later
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.JoinCondition;
import com.example.query.model.Query;
import com.example.query.result.ResultGenerationException;
import com.example.query.result.TableResultService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Table;
import tech.tablesaw.api.ColumnType;
// Statically import common column types for easier comparison
import static tech.tablesaw.api.ColumnType.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Handles the execution of JOIN operations between subquery results.
 */
public class JoinHandler {
    private static final Logger logger = LoggerFactory.getLogger(JoinHandler.class);

    private final JoinExecutor joinExecutor;
    private final TableResultService tableResultService;

    /**
     * Creates a new JoinHandler.
     *
     * @param joinExecutor       The executor responsible for performing table joins.
     * @param tableResultService The service for converting results to/from Tables.
     */
    public JoinHandler(JoinExecutor joinExecutor, TableResultService tableResultService) {
        this.joinExecutor = joinExecutor;
        this.tableResultService = tableResultService;
    }

    /**
     * Executes the join specified in the query using pre-computed subquery results.
     *
     * @param query           The query containing the join condition and subquery definitions.
     * @param indexes         Map of index names to IndexAccess (potentially needed for context).
     * @param subqueryContext Context containing the results of executed subqueries as Tables.
     * @param source          The source name associated with the main query.
     * @return A set of DocSentenceMatch representing the result of the join.
     * @throws QueryExecutionException if the join execution fails.
     */
    public Set<DocSentenceMatch> handleJoin(
            Query query,
            Map<String, IndexAccess> indexes,
            SubqueryContext subqueryContext,
            String source)
            throws QueryExecutionException {

        logger.debug("Handling JOIN operation for query from source: {}", source);

        JoinCondition joinCondition = query.joinCondition().orElseThrow(() ->
                new QueryExecutionException("Join condition is required but missing in JoinHandler",
                        "join", QueryExecutionException.ErrorType.INTERNAL_ERROR));

        // 1. Extract left/right subquery aliases from joinCondition columns.
        String leftColumnName = joinCondition.leftColumn();
        String rightColumnName = joinCondition.rightColumn();
        
        String leftAlias = extractAliasFromColumnName(leftColumnName);
        String rightAlias = extractAliasFromColumnName(rightColumnName);

        logger.debug("Joining subquery '{}' (left) with '{}' (right) on columns: {} and {}", 
                     leftAlias, rightAlias, leftColumnName, rightColumnName);

        // 2. Get the corresponding Tables from subqueryContext.
        if (!subqueryContext.hasResults(leftAlias) || !subqueryContext.hasResults(rightAlias)) {
            throw new QueryExecutionException(
                String.format("Missing results for one or both subqueries in JOIN: '%s', '%s'", leftAlias, rightAlias),
                "join", QueryExecutionException.ErrorType.INTERNAL_ERROR);
        }

        Table leftTable = subqueryContext.getTableResults(leftAlias);
        Table rightTable = subqueryContext.getTableResults(rightAlias);
        
        if (leftTable == null || rightTable == null) {
             throw new QueryExecutionException(
                String.format("Table results are null for one or both subqueries: '%s', '%s'", leftAlias, rightAlias),
                "join", QueryExecutionException.ErrorType.INTERNAL_ERROR);
        }
        
        logger.debug("Left table ('{}') has {} rows, Right table ('{}') has {} rows", 
                     leftAlias, leftTable.rowCount(), rightAlias, rightTable.rowCount());

        // 3. Execute the join using joinExecutor.join().
        try {
            Table joinedTable = joinExecutor.join(leftTable, rightTable, joinCondition);
            logger.debug("Join execution completed. Resulting table has {} rows.", joinedTable.rowCount());

            // 4. Convert the resulting Table back to Set<DocSentenceMatch>.
            return convertJoinedTableToMatches(joinedTable, source);

        } catch (Exception e) { // Catch potential exceptions during join execution or conversion
            logger.error("Error during join execution or result conversion: {}", e.getMessage(), e);
            throw new QueryExecutionException(
                "Failed to execute join or convert results: " + e.getMessage(),
                e,
                "join", 
                QueryExecutionException.ErrorType.INTERNAL_ERROR
            );
        }
    }

    // TODO: Move convertJoinedTableToMatches and extractAliasFromColumnName methods here from QueryExecutor.

    /**
     * Extracts the alias part from a column name in the format \"alias.column\".
     *
     * @param columnName The column name
     * @return The alias part
     * @throws QueryExecutionException if the column name does not contain an alias
     */
    private String extractAliasFromColumnName(String columnName) throws QueryExecutionException {
        if (columnName == null || !columnName.contains(".")) {
            throw new QueryExecutionException(
                String.format("Column name \'%s\' must be in the format \'alias.column\'", columnName),
                "join", QueryExecutionException.ErrorType.INVALID_CONDITION);
        }
        return columnName.substring(0, columnName.indexOf('.'));
    }

    /**
     * Converts a joined table back to a set of DocSentenceMatch objects.
     *
     * @param joinedTable The joined table
     * @param source      The source name for the matches
     * @return Set of DocSentenceMatch objects
     */
    private Set<DocSentenceMatch> convertJoinedTableToMatches(Table joinedTable, String source) {
        Set<DocSentenceMatch> matches = new HashSet<>();
        
        // Use constants for column names for robustness
        final String DOC_ID_COL = "document_id";
        final String SENT_ID_COL = "sentence_id"; // Optional column

        // Check if essential document_id column exists
        if (!joinedTable.columnNames().contains(DOC_ID_COL)) {
            logger.warn("Joined table does not contain '{}' column, cannot extract matches.", DOC_ID_COL);
            return matches;
        }
        
        boolean hasSentenceId = joinedTable.columnNames().contains(SENT_ID_COL);
        ColumnType docIdType = joinedTable.column(DOC_ID_COL).type();
        ColumnType sentIdType = hasSentenceId ? joinedTable.column(SENT_ID_COL).type() : null;

        // Convert each row to a DocSentenceMatch
        for (int i = 0; i < joinedTable.rowCount(); i++) {
            try {
                // Assuming document_id is stored as an integer or a string parsable to integer
                int documentId;
                // Check if the column type is one of the numeric types
                if (docIdType == INTEGER || docIdType == LONG || docIdType == SHORT || docIdType == DOUBLE || docIdType == FLOAT) { 
                    // Use getDouble() and cast to int for flexibility with different numeric types
                    documentId = (int) joinedTable.numberColumn(DOC_ID_COL).getDouble(i);
                } else {
                    documentId = Integer.parseInt(joinedTable.stringColumn(DOC_ID_COL).get(i));
                }

                int sentenceId = -1; // Default for document granularity or if column missing
                if (hasSentenceId && sentIdType != null) {
                    // Similarly handle sentence_id, checking type
                    if (sentIdType == INTEGER || sentIdType == LONG || sentIdType == SHORT || sentIdType == DOUBLE || sentIdType == FLOAT) {
                         // Use getDouble() and cast to int
                        sentenceId = (int) joinedTable.numberColumn(SENT_ID_COL).getDouble(i);
                    } else {
                        // Handle potential missing or non-integer values gracefully
                        String sentIdStr = joinedTable.stringColumn(SENT_ID_COL).get(i);
                        if (sentIdStr != null && !sentIdStr.isEmpty() && !sentIdStr.equalsIgnoreCase("null")) {
                            try {
                                sentenceId = Integer.parseInt(sentIdStr);
                            } catch (NumberFormatException nfe) {
                                logger.trace("Could not parse sentence_id '{}' in row {}, using default -1.", sentIdStr, i);
                                // sentenceId remains -1
                            }
                        }
                    } 
                }
                
                DocSentenceMatch match = new DocSentenceMatch(documentId, sentenceId, source);
                matches.add(match);
            } catch (Exception e) {
                // Catch broader exceptions during row processing
                logger.warn("Failed to process row {} for DocSentenceMatch conversion: {}", i, e.getMessage());
                // Skip this row
            }
        }
        
        logger.debug("Converted joined table to {} DocSentenceMatch objects.", matches.size());
        return matches;
    }
} 