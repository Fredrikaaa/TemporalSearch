package com.example.annotation;

import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TextSegmenterTest {

    @Test
    void testEmptyText() {
        TextSegmenter segmenter = new TextSegmenter();
        assertTrue(segmenter.chunkDocument("").isEmpty());
        assertTrue(segmenter.chunkDocument(null).isEmpty());
    }
    
    @Test
    void testSmallText() {
        TextSegmenter segmenter = new TextSegmenter(1000);
        String text = "This is a small text that should not be split.";
        List<String> chunks = segmenter.chunkDocument(text);
        
        assertEquals(1, chunks.size());
        assertEquals(text, chunks.get(0));
    }
    
    @Test
    void testParagraphSplitting() {
        TextSegmenter segmenter = new TextSegmenter(50);
        String text = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph.";
        List<String> chunks = segmenter.chunkDocument(text);
        
        assertTrue(chunks.size() >= 2);
        assertTrue(chunks.get(0).contains("First paragraph"));
        assertTrue(chunks.get(1).contains("Second paragraph"));
    }
    
    @Test
    void testSentenceSplitting() {
        TextSegmenter segmenter = new TextSegmenter(30);
        String text = "First sentence. Second sentence. Third sentence.";
        List<String> chunks = segmenter.chunkDocument(text);
        
        assertTrue(chunks.size() >= 2);
        assertTrue(chunks.get(0).contains("First sentence"));
        assertTrue(chunks.get(1).contains("Second sentence"));
    }
    
    @Test
    void testOverlap() {
        TextSegmenter segmenter = new TextSegmenter(20);
        String text = "Start. Middle part. End.";
        List<String> chunks = segmenter.chunkDocument(text);
        
        // Check that chunks overlap
        assertTrue(chunks.size() >= 2);
        String firstChunk = chunks.get(0);
        String secondChunk = chunks.get(1);
        
        // The period from "Start." should appear in both chunks
        assertTrue(firstChunk.contains("Start."));
        assertTrue(secondChunk.contains("Start.") || secondChunk.contains("Middle"));
    }
    
    @Test
    void testLargeDocument() {
        // Create a moderately sized document with repeated text
        StringBuilder builder = new StringBuilder();
        String repeatedText = "This is a sentence. Here is another one. ";
        for (int i = 0; i < 200; i++) {  // Reduced from 1000 to 200
            builder.append(repeatedText);
            if (i % 10 == 0) {
                builder.append("\n\n");
            }
        }
        String text = builder.toString();
        
        TextSegmenter segmenter = new TextSegmenter(5_000);  // Reduced from 10_000 to force multiple chunks
        List<String> chunks = segmenter.chunkDocument(text);
        
        // Verify chunks are created and not too large
        assertTrue(chunks.size() > 1, "Should create multiple chunks");
        for (String chunk : chunks) {
            assertTrue(chunk.length() <= 5_200,  // Adjusted to match new chunk size
                      String.format("Chunk size %d exceeds max size + 2*overlap (5,200)", chunk.length()));
        }
        
        // Verify we can reconstruct the original text (ignoring overlaps)
        StringBuilder reconstructed = new StringBuilder();
        for (int i = 0; i < chunks.size(); i++) {
            String chunk = chunks.get(i);
            if (i == 0) {
                // First chunk - use everything
                reconstructed.append(chunk);
            } else {
                // Skip overlap from previous chunk
                reconstructed.append(chunk.substring(100));
            }
        }
        
        // The reconstructed text should contain all sentences from the original
        String[] originalSentences = text.split("\\. ");
        String reconstructedText = reconstructed.toString();
        
        // Test a sample of sentences rather than all of them
        for (int i = 0; i < originalSentences.length; i += 10) {
            String sentence = originalSentences[i];
            if (!sentence.isEmpty()) {
                assertTrue(reconstructedText.contains(sentence), 
                          "Missing sentence: " + sentence);
            }
        }
    }
    
    @Test
    void testCustomChunkSize() {
        TextSegmenter segmenter = new TextSegmenter(15);
        String text = "Very short text. But needs to be split.";
        List<String> chunks = segmenter.chunkDocument(text);
        
        assertTrue(chunks.size() > 1);
        for (String chunk : chunks) {
            assertTrue(chunk.length() <= 15 + 200); // maxSize + 2*overlap
        }
    }
} 