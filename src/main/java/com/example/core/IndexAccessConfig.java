package com.example.core;

import org.iq80.leveldb.CompressionType;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for IndexAccess instances.
 * Provides settings for performance tuning and behavior customization.
 */
public class IndexAccessConfig {
    private final String indexType;
    private final int batchSize;
    private final int cacheSize;
    private final CompressionType compressionType;
    private final int maxRetries;
    private final Duration retryDelay;
    private final boolean syncWrites;
    private final Map<String, String> typeSpecificSettings;

    private IndexAccessConfig(Builder builder) {
        this.indexType = builder.indexType;
        this.batchSize = builder.batchSize;
        this.cacheSize = builder.cacheSize;
        this.compressionType = builder.compressionType;
        this.maxRetries = builder.maxRetries;
        this.retryDelay = builder.retryDelay;
        this.syncWrites = builder.syncWrites;
        this.typeSpecificSettings = new HashMap<>(builder.typeSpecificSettings);
    }

    public static class Builder {
        private String indexType;
        private int batchSize = 10_000;
        private int cacheSize = 64 * 1024 * 1024; // 64MB default
        private CompressionType compressionType = CompressionType.SNAPPY;
        private int maxRetries = 3;
        private Duration retryDelay = Duration.ofMillis(100);
        private boolean syncWrites = false;
        private Map<String, String> typeSpecificSettings = new HashMap<>();

        public Builder(String indexType) {
            this.indexType = indexType;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder cacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder compressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryDelay(Duration retryDelay) {
            this.retryDelay = retryDelay;
            return this;
        }

        public Builder syncWrites(boolean syncWrites) {
            this.syncWrites = syncWrites;
            return this;
        }

        public Builder addTypeSetting(String key, String value) {
            this.typeSpecificSettings.put(key, value);
            return this;
        }

        public IndexAccessConfig build() {
            validate();
            return new IndexAccessConfig(this);
        }

        private void validate() {
            if (indexType == null || indexType.isEmpty()) {
                throw new IllegalArgumentException("indexType must be specified");
            }
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive");
            }
            if (cacheSize <= 0) {
                throw new IllegalArgumentException("cacheSize must be positive");
            }
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must be non-negative");
            }
        }
    }

    // Getters
    public String getIndexType() { return indexType; }
    public int getBatchSize() { return batchSize; }
    public int getCacheSize() { return cacheSize; }
    public CompressionType getCompressionType() { return compressionType; }
    public int getMaxRetries() { return maxRetries; }
    public Duration getRetryDelay() { return retryDelay; }
    public boolean getSyncWrites() { return syncWrites; }
    public Map<String, String> getTypeSpecificSettings() { return new HashMap<>(typeSpecificSettings); }
} 