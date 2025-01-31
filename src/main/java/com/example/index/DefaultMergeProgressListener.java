package com.example.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.ProgressTracker;

/**
 * Default implementation of MergeProgressListener that integrates with ProgressTracker.
 * Provides progress tracking and logging for merge operations.
 */
public class DefaultMergeProgressListener implements MergeProgressListener {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMergeProgressListener.class);
    
    private final ProgressTracker progress;
    private final String phase;
    private long startTime;
    private long lastLogTime;
    private static final long LOG_INTERVAL_MS = 5000; // Log every 5 seconds
    
    /**
     * Creates a new DefaultMergeProgressListener.
     * @param progress The progress tracker to use
     * @param phase The name of the merge phase
     */
    public DefaultMergeProgressListener(ProgressTracker progress, String phase) {
        this.progress = progress;
        this.phase = phase;
    }
    
    @Override
    public void onMergeStart(int totalFiles) {
        startTime = System.currentTimeMillis();
        lastLogTime = startTime;
        progress.startIndex("Merging " + phase, totalFiles);
        logger.info("Starting merge phase '{}' with {} files", phase, totalFiles);
    }
    
    @Override
    public void onMergeProgress(int filesProcessed, long bytesProcessed) {
        progress.updateIndex(filesProcessed);
        
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLogTime >= LOG_INTERVAL_MS) {
            long elapsedSeconds = (currentTime - startTime) / 1000;
            if (elapsedSeconds > 0) {
                double filesPerSecond = filesProcessed / (double) elapsedSeconds;
                double mbProcessed = bytesProcessed / (1024.0 * 1024.0);
                double mbPerSecond = mbProcessed / elapsedSeconds;
                
                logger.info("Merge progress: {} files ({:.2f} files/sec), {:.2f}MB ({:.2f}MB/sec)",
                    filesProcessed, filesPerSecond, mbProcessed, mbPerSecond);
            }
            lastLogTime = currentTime;
        }
    }
    
    @Override
    public void onMergeComplete(long totalBytesProcessed) {
        long endTime = System.currentTimeMillis();
        long elapsedSeconds = (endTime - startTime) / 1000;
        double mbProcessed = totalBytesProcessed / (1024.0 * 1024.0);
        
        progress.completeIndex();
        
        if (elapsedSeconds > 0) {
            double mbPerSecond = mbProcessed / elapsedSeconds;
            logger.info("Merge phase '{}' completed: {:.2f}MB processed ({:.2f}MB/sec)",
                phase, mbProcessed, mbPerSecond);
        } else {
            logger.info("Merge phase '{}' completed: {:.2f}MB processed",
                phase, mbProcessed);
        }
    }
    
    @Override
    public void onMergeError(Throwable error) {
        logger.error("Error during merge phase '{}': {}", phase, error.getMessage(), error);
        // Since ProgressTracker doesn't have a direct error method, we'll just complete the phase
        progress.completeIndex();
    }
} 