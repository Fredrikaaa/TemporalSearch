package com.example.index;

/**
 * Interface for monitoring progress during external sort merge operations.
 * Provides callbacks for tracking merge progress and collecting metrics.
 */
public interface MergeProgressListener {
    /**
     * Called when a merge operation starts.
     * @param totalFiles The total number of files to be merged
     */
    void onMergeStart(int totalFiles);

    /**
     * Called periodically during merge operation to report progress.
     * @param filesProcessed Number of files processed so far
     * @param bytesProcessed Number of bytes processed so far
     */
    void onMergeProgress(int filesProcessed, long bytesProcessed);

    /**
     * Called when a merge operation completes.
     * @param totalBytesProcessed Total number of bytes processed during the merge
     */
    void onMergeComplete(long totalBytesProcessed);

    /**
     * Called when a merge operation encounters an error.
     * @param error The error that occurred
     */
    void onMergeError(Throwable error);
} 