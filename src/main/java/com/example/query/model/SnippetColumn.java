package com.example.query.model;

/**
 * Represents a SNIPPET expression in the SELECT clause of a query.
 * This column selects snippet context around matched terms.
 */
public class SnippetColumn implements SelectColumn {
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
    public String toString() {
        return snippetNode.toString();
    }
} 