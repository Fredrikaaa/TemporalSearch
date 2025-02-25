package com.example.query.snippet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Highlights matches in text
 */
public class Highlighter {
    private static final Logger logger = LoggerFactory.getLogger(Highlighter.class);
    private final String prefix;
    private final String suffix;

    /**
     * Creates a new highlighter with the given style
     * @param style Style to use for highlighting (e.g., "**" for bold)
     */
    public Highlighter(String style) {
        logger.debug("Creating highlighter with style: {}", style);
        if ("**".equals(style)) {
            this.prefix = "**";
            this.suffix = "**";
        } else {
            // Support other highlighting styles as needed
            this.prefix = style;
            this.suffix = style;
        }
    }

    /**
     * Highlights a match in text
     * @param text The text containing the match
     * @param matchStart Start position of the match
     * @param matchEnd End position of the match
     * @return Text with the match highlighted
     * @throws IllegalArgumentException if match positions are invalid
     */
    public String highlight(String text, int matchStart, int matchEnd) {
        if (matchStart < 0 || matchEnd > text.length() || matchStart >= matchEnd) {
            logger.error("Invalid match positions: start={}, end={}, textLength={}", 
                matchStart, matchEnd, text.length());
            throw new IllegalArgumentException("Invalid match positions");
        }

        String match = text.substring(matchStart, matchEnd);
        String highlighted = prefix + match + suffix;
        
        logger.debug("Highlighting text from position {} to {}: '{}'", 
            matchStart, matchEnd, match);
            
        return text.substring(0, matchStart) + highlighted + text.substring(matchEnd);
    }
} 