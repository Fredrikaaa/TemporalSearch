package com.example.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for external sort operations.
 * Provides settings for memory usage, buffer sizes, and merge parameters.
 */
public class ExternalSortConfig {
    private static final Logger logger = LoggerFactory.getLogger(ExternalSortConfig.class);

    private final int maxMemoryMb;
    private final int bufferSizeMb;
    private final int maxFilesPerMerge;
    private final int mergeThreads;

    private ExternalSortConfig(Builder builder) {
        this.maxMemoryMb = builder.maxMemoryMb;
        this.bufferSizeMb = builder.bufferSizeMb;
        this.maxFilesPerMerge = builder.maxFilesPerMerge;
        this.mergeThreads = builder.mergeThreads;
        
        logger.debug("Created ExternalSortConfig: maxMemory={}MB, bufferSize={}MB, maxFiles={}, threads={}",
            maxMemoryMb, bufferSizeMb, maxFilesPerMerge, mergeThreads);
    }

    public int getMaxMemoryMb() {
        return maxMemoryMb;
    }

    public int getBufferSizeMb() {
        return bufferSizeMb;
    }

    public int getMaxFilesPerMerge() {
        return maxFilesPerMerge;
    }

    public int getMergeThreads() {
        return mergeThreads;
    }

    public static class Builder {
        private int maxMemoryMb = 512;  // Default 512MB
        private int bufferSizeMb = 32;  // Default 32MB
        private int maxFilesPerMerge = 100;
        private int mergeThreads = Math.min(8, Runtime.getRuntime().availableProcessors());

        public Builder withMaxMemoryMb(int value) {
            if (value <= 0) {
                throw new IllegalArgumentException("maxMemoryMb must be positive");
            }
            this.maxMemoryMb = value;
            return this;
        }

        public Builder withBufferSizeMb(int value) {
            if (value <= 0) {
                throw new IllegalArgumentException("bufferSizeMb must be positive");
            }
            this.bufferSizeMb = value;
            return this;
        }

        public Builder withMaxFilesPerMerge(int value) {
            if (value <= 1) {
                throw new IllegalArgumentException("maxFilesPerMerge must be greater than 1");
            }
            this.maxFilesPerMerge = value;
            return this;
        }

        public Builder withMergeThreads(int value) {
            if (value <= 0) {
                throw new IllegalArgumentException("mergeThreads must be positive");
            }
            this.mergeThreads = value;
            return this;
        }

        public ExternalSortConfig build() {
            validate();
            return new ExternalSortConfig(this);
        }

        private void validate() {
            if (bufferSizeMb > maxMemoryMb) {
                throw new IllegalStateException("Buffer size cannot be larger than max memory");
            }
            if (maxMemoryMb < 64) {
                throw new IllegalStateException("Max memory must be at least 64MB");
            }
            if (bufferSizeMb < 4) {
                throw new IllegalStateException("Buffer size must be at least 4MB");
            }
            if (mergeThreads > Runtime.getRuntime().availableProcessors()) {
                logger.warn("Configured merge threads ({}) exceeds available processors ({})",
                    mergeThreads, Runtime.getRuntime().availableProcessors());
            }
        }
    }
} 

