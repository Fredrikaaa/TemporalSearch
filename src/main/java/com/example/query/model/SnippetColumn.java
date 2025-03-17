package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.executor.VariableBindings;
import com.example.query.sqlite.SqliteAccessor;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a SNIPPET expression in the SELECT clause of a query.
 * This column selects snippet context around matched terms.
 */
public class SnippetColumn implements SelectColumn {
    private static final Logger logger = LoggerFactory.getLogger(SnippetColumn.class);
    
    private final SnippetNode snippetNode;
    
    /**
     * Creates a new snippet column.
     * 
     * @param snippetNode The snippet node containing configuration
     */
    public SnippetColumn(SnippetNode snippetNode) {
        this.snippetNode = snippetNode;
    }
    
    /**
     * Gets the snippet node.
     * 
     * @return The snippet node
     */
    public SnippetNode getSnippetNode() {
        return snippetNode;
    }
    
    @Override
    public String getColumnName() {
        String variableName = snippetNode.variable();
        if (variableName.startsWith("?")) {
            variableName = variableName.substring(1);
        }
        return "snippet_" + variableName;
    }
    
    @Override
    public Column<?> createColumn() {
        return StringColumn.create(getColumnName());
    }
    
    @Override
    public void populateColumn(Table table, int rowIndex, DocSentenceMatch match, 
                              VariableBindings variableBindings, Map<String, IndexAccess> indexes) {
        StringColumn column = (StringColumn) table.column(getColumnName());
        
        // Extract variable name without ? prefix
        String variableName = snippetNode.variable();
        if (variableName.startsWith("?")) {
            variableName = variableName.substring(1);
        }
        
        // Get the source for this document
        String source = match.getSource();
        int documentId = match.documentId();
        
        // Get the variable value to find the match position
        String matchText = null;
        int sentenceId = match.isSentenceLevel() ? match.sentenceId() : -1;
        
        if (match.isSentenceLevel()) {
            Map<String, List<String>> sentVars = variableBindings.getValuesForSentence(documentId, sentenceId);
            List<String> values = sentVars.get(variableName);
            if (values != null && !values.isEmpty()) {
                matchText = values.get(0);
            }
        } else {
            Map<String, List<String>> docVars = variableBindings.getValuesForDocument(documentId);
            List<String> values = docVars.get(variableName);
            if (values != null && !values.isEmpty()) {
                matchText = values.get(0);
            }
        }
        
        if (matchText == null) {
            column.set(rowIndex, null);
            return;
        }
        
        // Get the document text using SqliteAccessor singleton
        String docText = SqliteAccessor.getInstance().getDocumentText(source, documentId);
        if (docText == null) {
            column.set(rowIndex, null);
            return;
        }
        
        // If sentence-level, get just the sentence text
        String contextText = docText;
        if (match.isSentenceLevel()) {
            // For sentence-level, we still need to get sentences from the index
            IndexAccess index = indexes.get(source);
            if (index != null) {
                String[] sentences = index.getDocumentSentences(documentId);
                if (sentences != null && sentenceId >= 0 && sentenceId < sentences.length) {
                    contextText = sentences[sentenceId];
                }
            }
        }
        
        // Find the match position and extract the snippet
        int matchPos = contextText.indexOf(matchText);
        if (matchPos >= 0) {
            int windowSize = snippetNode.windowSize();
            int start = Math.max(0, matchPos - windowSize);
            int end = Math.min(contextText.length(), matchPos + matchText.length() + windowSize);
            
            String snippet = contextText.substring(start, end);
            // Add ellipsis if truncated
            if (start > 0) snippet = "..." + snippet;
            if (end < contextText.length()) snippet = snippet + "...";
            
            column.set(rowIndex, snippet);
        } else {
            column.set(rowIndex, null);
        }
    }
    
    @Override
    public String toString() {
        return snippetNode.toString();
    }
} 