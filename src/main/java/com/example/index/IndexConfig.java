package com.example.index;

/**
 * Configuration class for index generation settings.
 * Handles settings related to index preservation and safety checks.
 */
public class IndexConfig {
    
    private final boolean preserveExistingIndex;
    private final long sizeThresholdForConfirmation;
    
    private IndexConfig(Builder builder) {
        this.preserveExistingIndex = builder.preserveExistingIndex;
        this.sizeThresholdForConfirmation = builder.sizeThresholdForConfirmation;
    }
    
    public boolean shouldPreserveExistingIndex() {
        return preserveExistingIndex;
    }
    
    public long getSizeThresholdForConfirmation() {
        return sizeThresholdForConfirmation;
    }
    
    public static class Builder {
        private boolean preserveExistingIndex = false;
        private long sizeThresholdForConfirmation = 1024 * 1024 * 1024; // 1GB
        
        public Builder withPreserveExistingIndex(boolean preserve) {
            this.preserveExistingIndex = preserve;
            return this;
        }
        
        public Builder withSizeThresholdForConfirmation(long threshold) {
            if (threshold <= 0) {
                throw new IllegalArgumentException("Size threshold must be positive");
            }
            this.sizeThresholdForConfirmation = threshold;
            return this;
        }
        
        public IndexConfig build() {
            return new IndexConfig(this);
        }
    }
} 