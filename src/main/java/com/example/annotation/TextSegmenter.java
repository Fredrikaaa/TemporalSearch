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
    public static final int OVERLAP_SIZE = 100;
    
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
            // Find end position ensuring we don't exceed maxChunkSize
            int endPos = Math.min(startPos + maxChunkSize, text.length());
            
            // If we're not at the end of the text, find a better break point
            if (endPos < text.length()) {
                endPos = findBreakPoint(text, startPos, endPos);
            }
            
            // Extract the chunk with proper handling of overlap
            String chunk = extractChunkWithOverlap(text, startPos, endPos);
            chunks.add(chunk);
            
            // Move start position for next chunk, accounting for overlap
            startPos = endPos;
        }
        
        logger.debug("Split text of length {} into {} chunks", text.length(), chunks.size());
        return chunks;
    }
    
    /**
     * Splits a document into chunks suitable for parallel processing
     * and tracks the starting position of each chunk in the original document
     * 
     * @param text The document text to split
     * @return ChunkResult containing text chunks and their starting positions
     */
    public ChunkResult chunkDocumentWithPositions(String text) {
        if (text == null || text.isEmpty()) {
            return new ChunkResult(List.of(), List.of());
        }
        
        List<String> chunks = new ArrayList<>();
        List<Integer> startPositions = new ArrayList<>();
        int startPos = 0;
        
        while (startPos < text.length()) {
            // Record the starting position of this chunk
            startPositions.add(startPos);
            
            // Find end position ensuring we don't exceed maxChunkSize
            int endPos = Math.min(startPos + maxChunkSize, text.length());
            
            // If we're not at the end of the text, find a better break point
            if (endPos < text.length()) {
                endPos = findBreakPoint(text, startPos, endPos);
            }
            
            // Extract the chunk with proper handling of overlap
            String chunk = extractChunkWithOverlap(text, startPos, endPos);
            chunks.add(chunk);
            
            // Move start position for next chunk, accounting for overlap
            startPos = endPos;
        }
        
        logger.debug("Split text of length {} into {} chunks with position tracking", 
                text.length(), chunks.size());
        return new ChunkResult(chunks, startPositions);
    }
    
    /**
     * Result class containing chunks and their starting positions in the original document
     */
    public static class ChunkResult {
        private final List<String> chunks;
        private final List<Integer> startPositions;
        
        public ChunkResult(List<String> chunks, List<Integer> startPositions) {
            this.chunks = chunks;
            this.startPositions = startPositions;
        }
        
        public List<String> getChunks() {
            return chunks;
        }
        
        public List<Integer> getStartPositions() {
            return startPositions;
        }
        
        public int getStartPosition(int chunkIndex) {
            if (chunkIndex < 0 || chunkIndex >= startPositions.size()) {
                throw new IndexOutOfBoundsException("Invalid chunk index: " + chunkIndex);
            }
            return startPositions.get(chunkIndex);
        }
    }
    
    /**
     * Finds a suitable breakpoint in text near the target position,
     * preferring natural boundaries like sentence or paragraph breaks
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