package com.example.annotation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Handles text segmentation for efficient document processing.
 * Splits large documents into manageable chunks while preserving context
 * around chunk boundaries.
 */
public class TextSegmenter {
    private static final Logger logger = LoggerFactory.getLogger(TextSegmenter.class);
    
    // Default chunk size of 10k characters as specified in design
    private static final int DEFAULT_CHUNK_SIZE = 10_000;
    
    // Overlap buffer to prevent cutting sentences/entities
    private static final int OVERLAP_SIZE = 100;
    
    // Pattern for finding natural break points (paragraphs, sections)
    private static final Pattern BREAK_PATTERN = Pattern.compile("\\n\\n+|(?<=[.!?])\\s+(?=[A-Z])");
    
    private final int maxChunkSize;
    
    /**
     * Creates a new TextSegmenter with default chunk size
     */
    public TextSegmenter() {
        this(DEFAULT_CHUNK_SIZE);
    }
    
    /**
     * Creates a new TextSegmenter with specified chunk size
     * @param maxChunkSize Maximum size of each chunk in characters
     */
    public TextSegmenter(int maxChunkSize) {
        this.maxChunkSize = maxChunkSize;
        logger.debug("Created TextSegmenter with max chunk size: {}", maxChunkSize);
    }
    
    /**
     * Splits a document into chunks suitable for parallel processing
     * @param text The document text to split
     * @return List of text chunks with overlap for boundary handling
     */
    public List<String> chunkDocument(String text) {
        if (text == null || text.isEmpty()) {
            return List.of();
        }
        
        List<String> chunks = new ArrayList<>();
        int startPos = 0;
        
        while (startPos < text.length()) {
            // Calculate the maximum possible end position for this chunk
            int maxEnd = Math.min(startPos + maxChunkSize, text.length());
            int endPos = maxEnd;
            
            // If we're not at the end, find a natural break point
            if (endPos < text.length()) {
                int breakPoint = findBreakPoint(text, startPos, endPos);
                // Only use break point if it would create a reasonable chunk
                if (breakPoint > startPos && breakPoint < maxEnd) {
                    endPos = breakPoint;
                }
            }
            
            // Extract the chunk with overlap
            String chunk = extractChunkWithOverlap(text, startPos, endPos);
            
            // Verify the chunk size is within limits
            if (chunk.length() > maxChunkSize + OVERLAP_SIZE) {
                // If too large, force a break at maxSize - overlap
                endPos = startPos + maxChunkSize - OVERLAP_SIZE;
                chunk = extractChunkWithOverlap(text, startPos, endPos);
            }
            
            chunks.add(chunk);
            
            // Move to next chunk, ensuring we make progress
            int newStartPos = endPos;
            if (newStartPos <= startPos) {
                // Force progress if we're stuck
                newStartPos = Math.min(startPos + maxChunkSize / 2, text.length());
            }
            startPos = newStartPos;
            
            logger.trace("Created chunk {} of {} chars", chunks.size(), chunk.length());
        }
        
        logger.debug("Split document of {} chars into {} chunks", text.length(), chunks.size());
        return chunks;
    }
    
    /**
     * Finds a natural break point in the text near the target position
     * @param text The text to search in
     * @param start Start position of the current chunk
     * @param target Target position to find break point near
     * @return Position of the best break point
     */
    private int findBreakPoint(String text, int start, int target) {
        // Look for break points in a window around the target
        int windowStart = Math.max(0, Math.min(text.length() - 1, target - OVERLAP_SIZE));
        int windowEnd = Math.min(text.length(), target + OVERLAP_SIZE);
        
        // Ensure window start is after chunk start
        windowStart = Math.max(start, windowStart);
        
        // If window is invalid, return target
        if (windowStart >= windowEnd) {
            return target;
        }
        
        String window = text.substring(windowStart, windowEnd);
        var matcher = BREAK_PATTERN.matcher(window);
        
        // Find all break points in the window
        int bestBreak = -1;
        int bestDistance = Integer.MAX_VALUE;
        
        while (matcher.find()) {
            int breakPoint = windowStart + matcher.start();
            int distance = Math.abs(breakPoint - target);
            
            // Keep the break point closest to target
            if (distance < bestDistance) {
                bestBreak = breakPoint;
                bestDistance = distance;
            }
        }
        
        // If we found a break point, use it
        if (bestBreak != -1) {
            return bestBreak;
        }
        
        // If no natural break point found, return the target position
        return target;
    }
    
    /**
     * Extracts a chunk of text with overlap for boundary handling
     * @param text The source text
     * @param start Start position of the chunk
     * @param end End position of the chunk
     * @return The extracted chunk with overlap
     */
    private String extractChunkWithOverlap(String text, int start, int end) {
        // Calculate overlap boundaries, ensuring we don't exceed text bounds
        int chunkStart = Math.max(0, start - (start > 0 ? OVERLAP_SIZE : 0));
        int chunkEnd = Math.min(text.length(), end + (end < text.length() ? OVERLAP_SIZE : 0));
        
        return text.substring(chunkStart, chunkEnd);
    }
} 