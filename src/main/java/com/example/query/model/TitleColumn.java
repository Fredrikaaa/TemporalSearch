package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.executor.VariableBindings;
import com.example.query.sqlite.SqliteAccessor;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a column that displays document titles.
 */
public class TitleColumn implements SelectColumn {
    private static final Logger logger = LoggerFactory.getLogger(TitleColumn.class);
    
    /**
     * Creates a new title column.
     */
    public TitleColumn() {
        // No configuration needed
    }
    
    @Override
    public String getColumnName() {
        return "title";
    }
    
    @Override
    public Column<?> createColumn() {
        return StringColumn.create(getColumnName());
    }
    
    @Override
    public void populateColumn(Table table, int rowIndex, DocSentenceMatch match, 
                              VariableBindings variableBindings, Map<String, IndexAccess> indexes) {
        StringColumn column = (StringColumn) table.column(getColumnName());
        
        // Get the source for this document
        String source = match.getSource();
        int documentId = match.documentId();
        logger.debug("Getting title for document {} from source {}", documentId, source);
        
        // Get title using the SqliteAccessor singleton
        String value = SqliteAccessor.getInstance().getMetadata(source, documentId, "title");
        
        column.set(rowIndex, value != null ? value : "");
    }
    
    @Override
    public String toString() {
        return "TITLE";
    }
} 