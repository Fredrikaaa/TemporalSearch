package com.example.logging;

import me.tongfei.progressbar.*;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages nested progress bars for index generation processes.
 * Provides tracking for overall progress, individual index progress, and batch progress.
 */
public class ProgressTracker implements AutoCloseable {
    private ProgressBar overallProgress;
    private ProgressBar currentIndexProgress;
    private ProgressBar batchProgress;
    private final AtomicLong overallCount = new AtomicLong(0);
    private final AtomicLong indexCount = new AtomicLong(0);
    private final AtomicLong batchCount = new AtomicLong(0);

    public void startOverall(String task, long total) {
        overallProgress = new ProgressBarBuilder()
            .setTaskName(task)
            .setInitialMax(total)
            .setStyle(ProgressBarStyle.ASCII)
            .setUpdateIntervalMillis(100)
            .showSpeed()
            .build();
    }

    public void startIndex(String indexType, long total) {
        if (currentIndexProgress != null) {
            currentIndexProgress.close();
        }
        indexCount.set(0);
        currentIndexProgress = new ProgressBarBuilder()
            .setTaskName("Generating " + indexType + " index")
            .setInitialMax(total)
            .setStyle(ProgressBarStyle.ASCII)
            .setUpdateIntervalMillis(100)
            .showSpeed()
            .build();
    }

    public void startBatch(long total) {
        if (batchProgress != null) {
            batchProgress.close();
        }
        batchCount.set(0);
        batchProgress = new ProgressBarBuilder()
            .setTaskName("Processing batch")
            .setInitialMax(total)
            .setStyle(ProgressBarStyle.ASCII)
            .setUpdateIntervalMillis(100)
            .showSpeed()
            .build();
    }

    public void updateOverall(long step) {
        if (overallProgress != null) {
            overallProgress.stepBy(step);
            overallCount.addAndGet(step);
        }
    }

    public void updateIndex(long step) {
        if (currentIndexProgress != null) {
            currentIndexProgress.stepBy(step);
            indexCount.addAndGet(step);
        }
    }

    public void updateBatch(long step) {
        if (batchProgress != null) {
            batchProgress.stepBy(step);
            batchCount.addAndGet(step);
        }
    }

    public void completeOverall() {
        if (overallProgress != null) {
            overallProgress.stepTo(overallProgress.getMax());
            overallProgress.close();
            overallProgress = null;
        }
    }

    public void completeIndex() {
        if (currentIndexProgress != null) {
            currentIndexProgress.stepTo(currentIndexProgress.getMax());
            currentIndexProgress.close();
            currentIndexProgress = null;
        }
    }

    public void completeBatch() {
        if (batchProgress != null) {
            batchProgress.stepTo(batchProgress.getMax());
            batchProgress.close();
            batchProgress = null;
        }
    }

    @Override
    public void close() {
        completeBatch();
        completeIndex();
        completeOverall();
    }
} 