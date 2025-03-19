package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.binding.BindingContext;
import com.example.query.sqlite.SqliteAccessor;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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
                              BindingContext bindingContext, Map<String, IndexAccess> indexes) {
        StringColumn column = (StringColumn) table.column(getColumnName());
        
        System.out.println("SnippetColumn: Populating column for variable: " + snippetNode.variable() + ", match: " + match);
        
        // Extract variable name without ? prefix
        String variableName = snippetNode.variable();
        if (!variableName.startsWith("?")) {
            variableName = "?" + variableName;
        }
        
        // Get the source for this document
        String source = match.getSource();
        int documentId = match.documentId();
        
        // Get the variable value to find the match position
        String variableValue = null;
        int sentenceId = match.isSentenceLevel() ? match.sentenceId() : -1;
        
        // Try to get a String value for this variable
        Optional<String> valueOpt = bindingContext.getValue(variableName, String.class);
        
        if (valueOpt.isPresent()) {
            variableValue = valueOpt.get();
            System.out.println("SnippetColumn: Found value: " + variableValue);
        } else {
            // Try to get all values for this variable
            List<String> values = bindingContext.getValues(variableName, String.class);
            if (!values.isEmpty()) {
                variableValue = values.get(0);
                System.out.println("SnippetColumn: Found value from list: " + variableValue);
            } else {
                System.out.println("SnippetColumn: No value found for variable: " + variableName);
            }
        }
        
        if (variableValue == null) {
            System.out.println("SnippetColumn: No value found for variable: " + variableName);
            column.set(rowIndex, null);
            return;
        }
        
        // Extract the term and position from the format "term@beginPos:endPos"
        String matchText;
        int beginPos = -1;
        int endPos = -1;
        
        int atIndex = variableValue.indexOf('@');
        int colonIndex = variableValue.indexOf(':', atIndex);
        
        if (atIndex > 0 && colonIndex > atIndex) {
            matchText = variableValue.substring(0, atIndex);
            try {
                beginPos = Integer.parseInt(variableValue.substring(atIndex + 1, colonIndex));
                endPos = Integer.parseInt(variableValue.substring(colonIndex + 1));
                System.out.println("SnippetColumn: Parsed position info: term=" + matchText + ", begin=" + beginPos + ", end=" + endPos);
            } catch (NumberFormatException e) {
                logger.warn("Failed to parse position info from variable value: {}", variableValue);
                matchText = variableValue; // Use the whole value as fallback
                System.out.println("SnippetColumn: Failed to parse position, using full value: " + matchText);
            }
        } else {
            matchText = variableValue; // Use the whole value if it doesn't follow the expected format
            System.out.println("SnippetColumn: No position info in value, using full value: " + matchText);
        }
        
        // Get the document text using SqliteAccessor singleton
        String docText = SqliteAccessor.getInstance().getDocumentText(source, documentId);
        if (docText == null) {
            System.out.println("SnippetColumn: No document text found for source=" + source + ", doc=" + documentId);
            column.set(rowIndex, null);
            return;
        }
        
        // If sentence-level, get just the sentence text
        String contextText = docText;
        if (sentenceId >= 0) { // Use sentenceId whether from match or found binding
            // For sentence-level, we still need to get sentences from the index
            IndexAccess index = indexes.get(source);
            if (index != null) {
                String[] sentences = index.getDocumentSentences(documentId);
                if (sentences != null && sentenceId >= 0 && sentenceId < sentences.length) {
                    contextText = sentences[sentenceId];
                    System.out.println("SnippetColumn: Using sentence context: " + contextText);
                    // Adjust beginPos and endPos for sentence context if we have valid positions
                    if (beginPos >= 0 && endPos >= 0) {
                        // Find the offset of this sentence in the document
                        int sentenceOffset = docText.indexOf(contextText);
                        if (sentenceOffset >= 0) {
                            beginPos -= sentenceOffset;
                            endPos -= sentenceOffset;
                            // Ensure positions are within bounds
                            beginPos = Math.max(0, beginPos);
                            endPos = Math.min(contextText.length(), endPos);
                            System.out.println("SnippetColumn: Adjusted positions for sentence: begin=" + beginPos + ", end=" + endPos);
                        }
                    }
                }
            }
        }
        
        // Create the snippet
        String snippet;
        
        // If we have valid position information, use it directly
        if (beginPos >= 0 && endPos >= 0 && beginPos < contextText.length()) {
            int windowSize = snippetNode.windowSize();
            int start = Math.max(0, beginPos - windowSize);
            int end = Math.min(contextText.length(), endPos + windowSize);
            
            snippet = contextText.substring(start, end);
            // Add ellipsis if truncated
            if (start > 0) snippet = "..." + snippet;
            if (end < contextText.length()) snippet = snippet + "...";
            System.out.println("SnippetColumn: Created snippet from positions: " + snippet);
        } else {
            // Fallback to searching for the term in the text
            int matchPos = contextText.indexOf(matchText);
            if (matchPos >= 0) {
                int windowSize = snippetNode.windowSize();
                int start = Math.max(0, matchPos - windowSize);
                int end = Math.min(contextText.length(), matchPos + matchText.length() + windowSize);
                
                snippet = contextText.substring(start, end);
                // Add ellipsis if truncated
                if (start > 0) snippet = "..." + snippet;
                if (end < contextText.length()) snippet = snippet + "...";
                System.out.println("SnippetColumn: Created snippet by searching for term: " + snippet);
            } else {
                // If we can't find the term, just take a section of the context
                int windowSize = snippetNode.windowSize();
                int middle = contextText.length() / 2;
                int start = Math.max(0, middle - windowSize);
                int end = Math.min(contextText.length(), middle + windowSize);
                
                snippet = contextText.substring(start, end);
                // Add ellipsis if truncated
                if (start > 0) snippet = "..." + snippet;
                if (end < contextText.length()) snippet = snippet + "...";
                
                // Add a note that the term wasn't found
                snippet += " [Note: Term '" + matchText + "' not found in text]";
                System.out.println("SnippetColumn: Created generic snippet, term not found: " + snippet);
            }
        }
        
        column.set(rowIndex, snippet);
    }
    
    @Override
    public String toString() {
        return snippetNode.toString();
    }
} 