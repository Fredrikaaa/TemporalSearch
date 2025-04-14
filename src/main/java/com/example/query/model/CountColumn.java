package com.example.query.model;

import com.example.core.IndexAccessInterface;
import com.example.query.binding.MatchDetail;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.selection.Selection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a COUNT expression in the SELECT clause of a query.
 * This column performs counting operations using Tablesaw's built-in aggregation features.
 */
public class CountColumn implements SelectColumn {
    private static final Logger logger = LoggerFactory.getLogger(CountColumn.class);

    public enum CountType {
        ALL,       // COUNT(*)
        UNIQUE,    // COUNT(UNIQUE ?var)
        DOCUMENTS  // COUNT(DOCUMENTS)
    }
    
    private final CountType type;
    private final String variable;
    private final String columnName;
    private final ColumnType columnType = ColumnType.INTEGER;
    
    /**
     * Creates a COUNT(*) column.
     */
    public static CountColumn countAll() {
        return new CountColumn(CountType.ALL, null, "count");
    }
    
    /**
     * Creates a COUNT(UNIQUE ?var) column.
     *
     * @param variable The variable to count unique values of (should include ? prefix)
     */
    public static CountColumn countUnique(String variable) {
        String colName = "count_" + (variable.startsWith("?") ? variable.substring(1) : variable);
        return new CountColumn(CountType.UNIQUE, variable, colName);
    }
    
    /**
     * Creates a COUNT(DOCUMENTS) column.
     */
    public static CountColumn countDocuments() {
        return new CountColumn(CountType.DOCUMENTS, null, "document_count");
    }
    
    /**
     * Creates a new count column.
     *
     * @param type The type of count operation
     * @param variable The variable to count (for UNIQUE counts, with ? prefix)
     * @param columnName The name for the column in the result table
     */
    private CountColumn(CountType type, String variable, String columnName) {
        this.type = type;
        this.variable = variable;
        this.columnName = columnName;
    }
    
    /**
     * Returns the variable name (with '?') if this is a COUNT(UNIQUE ?var) column,
     * otherwise returns null. Used for validation purposes.
     */
    public String getVariableNameForValidation() {
        return (type == CountType.UNIQUE) ? variable : null;
    }
    
    @Override
    public String getColumnName() {
        return columnName;
    }
    
    @Override
    public Column<?> createColumn() {
        return IntColumn.create(columnName);
    }
    
    @Override
    public void populateColumn(Table table, int rowIndex, List<MatchDetail> detailsForUnit, 
                               String source,
                               Map<String, IndexAccessInterface> indexes) {
        // COUNT columns are handled by a separate aggregation step after initial table population.
        // This method doesn't need to do anything during the row-by-row population phase.
        logger.trace("populateColumn called for CountColumn '{}', row {}. No direct population needed.", columnName, rowIndex);
    }
    
    /**
     * Applies count aggregations to a table.
     * Assumes the table is already populated with individual match details.
     * Groups by all columns *except* the COUNT column(s) and calculates counts.
     *
     * @param inputTable The table populated with raw match details.
     * @return A new table with aggregated counts.
     */
    public static Table applyCountAggregations(Table inputTable) {
        List<String> countColumns = inputTable.columns().stream()
            .filter(col -> col.type() == ColumnType.INTEGER && col.name().toUpperCase().startsWith("COUNT("))
            .map(Column::name)
            .toList();

        if (countColumns.isEmpty()) {
            logger.debug("No COUNT columns found, returning original table.");
            return inputTable;
        }

        List<String> groupColumns = inputTable.columns().stream()
            .map(Column::name)
            .filter(name -> countColumns.stream().noneMatch(countCol -> countCol.equalsIgnoreCase(name)))
            .toList();

        if (groupColumns.isEmpty()) {
            logger.warn("Cannot perform COUNT aggregation without non-count columns to group by.");
            // Or should we return a single row with the total count?
            // Let's return original for now.
            return inputTable;
        }

        logger.debug("Applying COUNT aggregation, grouping by: {}, counting: {}", groupColumns, countColumns);

        // Use Tablesaw built-in count aggregation function
        Table aggregatedTable = inputTable.summarize(countColumns.get(0), AggregateFunctions.count).by(groupColumns.toArray(String[]::new));
        
        // Rename the default "Count [col]" column to the original COUNT column name
        if (aggregatedTable.columnNames().contains("Count [" + countColumns.get(0) + "]")) {
            aggregatedTable.column("Count [" + countColumns.get(0) + "]").setName(countColumns.get(0));
        }

        // Handle multiple COUNT columns if necessary (though usually just one)
        // The basic summarize might only handle one aggregate column well.
        // If multiple count columns were present, they might need separate aggregations
        // or a different approach.
        if (countColumns.size() > 1) {
            logger.warn("Handling multiple COUNT columns ({}) with basic aggregation. Results might be unexpected.", countColumns);
        }

        logger.debug("Aggregation complete, resulting table has {} rows.", aggregatedTable.rowCount());
        return aggregatedTable;
    }
    
    @Override
    public String toString() {
        return switch (type) {
            case ALL -> "COUNT(*)";
            case UNIQUE -> "COUNT(UNIQUE " + variable + ")";
            case DOCUMENTS -> "COUNT(DOCUMENTS)";
        };
    }
} 