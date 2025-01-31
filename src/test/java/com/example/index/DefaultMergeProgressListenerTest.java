package com.example.index;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.example.logging.ProgressTracker;

@ExtendWith(MockitoExtension.class)
class DefaultMergeProgressListenerTest {
    
    @Mock
    private ProgressTracker progress;
    
    private DefaultMergeProgressListener listener;
    private static final String TEST_PHASE = "test_phase";
    
    @BeforeEach
    void setUp() {
        listener = new DefaultMergeProgressListener(progress, TEST_PHASE);
    }
    
    @Test
    void testMergeStart() {
        listener.onMergeStart(100);
        verify(progress).startIndex("Merging " + TEST_PHASE, 100);
    }
    
    @Test
    void testMergeProgress() {
        listener.onMergeProgress(50, 1024 * 1024); // 1MB processed
        verify(progress).updateIndex(50);
    }
    
    @Test
    void testMergeComplete() {
        listener.onMergeComplete(1024 * 1024 * 100); // 100MB processed
        verify(progress).completeIndex();
    }
    
    @Test
    void testMergeError() {
        Exception testError = new RuntimeException("Test error");
        listener.onMergeError(testError);
        verify(progress).completeIndex();
    }
    
    @Test
    void testProgressUpdatesWithDelay() throws InterruptedException {
        // First update
        listener.onMergeProgress(25, 1024 * 1024);
        verify(progress).updateIndex(25);
        
        // Second update immediately after - should still update progress but not log
        listener.onMergeProgress(50, 2 * 1024 * 1024);
        verify(progress).updateIndex(50);
        
        // Wait for log interval and update again
        Thread.sleep(5100); // Just over 5 seconds
        listener.onMergeProgress(75, 3 * 1024 * 1024);
        verify(progress).updateIndex(75);
    }
    
    @Test
    void testFullMergeLifecycle() {
        // Start merge
        listener.onMergeStart(100);
        verify(progress).startIndex("Merging " + TEST_PHASE, 100);
        
        // Update progress a few times
        listener.onMergeProgress(25, 1024 * 1024 * 25);
        verify(progress).updateIndex(25);
        
        listener.onMergeProgress(50, 1024 * 1024 * 50);
        verify(progress).updateIndex(50);
        
        listener.onMergeProgress(75, 1024 * 1024 * 75);
        verify(progress).updateIndex(75);
        
        // Complete merge
        listener.onMergeComplete(1024 * 1024 * 100);
        verify(progress).completeIndex();
        
        // Verify order of operations
        inOrder(progress).verify(progress).startIndex("Merging " + TEST_PHASE, 100);
        inOrder(progress).verify(progress).updateIndex(25);
        inOrder(progress).verify(progress).updateIndex(50);
        inOrder(progress).verify(progress).updateIndex(75);
        inOrder(progress).verify(progress).completeIndex();
    }
} 