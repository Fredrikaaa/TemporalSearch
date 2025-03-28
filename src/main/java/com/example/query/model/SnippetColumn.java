package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.binding.BindingContext;
import com.example.query.executor.NerExecutor;
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
        
        logger.debug("Populating column for variable: {} for match: {}", snippetNode.variable(), match);
        
        // Extract variable name without ? prefix
        String variableName = snippetNode.variable();
        if (!variableName.startsWith("?")) {
            variableName = "?" + variableName;
        }
        
        // Get the source for this document
        String source = match.getSource();
        int documentId = match.documentId();
        
        // Get the variable value to find the match position
        String matchText = null;
        int beginPos = -1;
        int endPos = -1;
        
        // First try to get the match-specific variable value
        try {
            // Check if this match has a specific value for this variable
            Object matchValue = match.getVariableValue(variableName);
            if (matchValue != null) {
                if (matchValue instanceof NerExecutor.MatchedEntityValue entityValue) {
                    // Get direct values from MatchedEntityValue
                    matchText = entityValue.text();
                    beginPos = entityValue.beginPosition();
                    endPos = entityValue.endPosition();
                    logger.debug("Found match-specific MatchedEntityValue with position info: text={}, begin={}, end={}", 
                               matchText, beginPos, endPos);
                } else if (matchValue instanceof String) {
                    matchText = (String) matchValue;
                    logger.debug("Found match-specific String value: {}", matchText);
                } else {
                    matchText = matchValue.toString();
                    logger.debug("Found match-specific value (converted to string): {}", matchText);
                }
            }
        } catch (Exception e) {
            logger.warn("Error extracting match-specific variable value: {}", e.getMessage());
        }
        
        // If no match-specific value, try the global binding context
        if (matchText == null) {
            try {
                // Try to get a MatchedEntityValue from binding context
                Optional<NerExecutor.MatchedEntityValue> entityValueOpt = 
                    bindingContext.getValue(variableName, NerExecutor.MatchedEntityValue.class);
                
                if (entityValueOpt.isPresent()) {
                    NerExecutor.MatchedEntityValue entityValue = entityValueOpt.get();
                    matchText = entityValue.text();
                    beginPos = entityValue.beginPosition();
                    endPos = entityValue.endPosition();
                    logger.debug("Found MatchedEntityValue from binding context: text={}, begin={}, end={}", 
                               matchText, beginPos, endPos);
                } else {
                    // Try to get a String value
                    Optional<String> stringOpt = bindingContext.getValue(variableName, String.class);
                    if (stringOpt.isPresent()) {
                        matchText = stringOpt.get();
                        logger.debug("Found String value from binding context: {}", matchText);
                    } else {
                        // Try to get any object as last resort
                        Optional<?> valueOpt = bindingContext.getValue(variableName, Object.class);
                        if (valueOpt.isPresent()) {
                            Object obj = valueOpt.get();
                            matchText = obj.toString();
                            logger.debug("Found value (converted to string) from binding context: {}", matchText);
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Error extracting variable value from binding context: {}", e);
            }
        }
        
        if (matchText == null) {
            logger.debug("No value found for variable: {}", variableName);
            column.set(rowIndex, null);
            return;
        }
        
        // Get the document text using SqliteAccessor singleton
        String docText = SqliteAccessor.getInstance().getDocumentText(source, documentId);
        if (docText == null) {
            logger.debug("No document text found for source={}, doc={}", source, documentId);
            column.set(rowIndex, null);
            return;
        }
        
        // If sentence-level, get just the sentence text
        String contextText = docText;
        int sentenceId = match.isSentenceLevel() ? match.sentenceId() : -1;
        
        if (sentenceId >= 0) { // Use sentenceId whether from match or found binding
            // For sentence-level, we still need to get sentences from the index
            IndexAccess index = indexes.get(source);
            if (index != null) {
                String[] sentences = index.getDocumentSentences(documentId);
                if (sentences != null && sentenceId >= 0 && sentenceId < sentences.length) {
                    contextText = sentences[sentenceId];
                    logger.debug("Using sentence context: {}", contextText);
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
                            logger.debug("Adjusted positions for sentence: begin={}, end={}", beginPos, endPos);
                        }
                    }
                }
            }
        }
        
        // Create the snippet
        String snippet;
        
        // If we have valid position information, use it directly
        if (beginPos >= 0 && endPos >= 0 && beginPos < contextText.length()) {
            int windowSize = snippetNode.windowSize()*10;
            int start = Math.max(0, beginPos - windowSize);
            int end = Math.min(contextText.length(), endPos + windowSize);
            
            snippet = contextText.substring(start, end);
            // Add ellipsis if truncated
            if (start > 0) snippet = "..." + snippet;
            if (end < contextText.length()) snippet = snippet + "...";
            logger.debug("Created snippet from positions: {}", snippet);
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
                logger.debug("Created snippet by searching for term: {}", snippet);
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
                logger.debug("Created generic snippet, term not found: {}", snippet);
            }
        }
        
        column.set(rowIndex, snippet);
    }
    
    @Override
    public String toString() {
        return snippetNode.toString();
    }
} 