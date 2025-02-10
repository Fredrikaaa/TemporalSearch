package com.example.core;

import java.nio.file.Path;
import java.util.Set;

/**
 * Shared configuration for both index generation and query operations.
 * Provides settings that affect both components.
 */
public class IndexConfig {
    private final Path indexRootPath;
    private final Path stopwordsPath;
    private final Set<String> indexTypes;
    private final boolean preserveExisting;
    private final int batchSize;
    private final long sizeThreshold;

    private IndexConfig(Builder builder) {
        this.indexRootPath = builder.indexRootPath;
        this.stopwordsPath = builder.stopwordsPath;
        this.indexTypes = builder.indexTypes;
        this.preserveExisting = builder.preserveExisting;
        this.batchSize = builder.batchSize;
        this.sizeThreshold = builder.sizeThreshold;
    }

    public Path getIndexRootPath() {
        return indexRootPath;
    }

    public Path getStopwordsPath() {
        return stopwordsPath;
    }

    public Set<String> getIndexTypes() {
        return indexTypes;
    }

    public boolean shouldPreserveExisting() {
        return preserveExisting;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getSizeThreshold() {
        return sizeThreshold;
    }

    public static class Builder {
        private Path indexRootPath;
        private Path stopwordsPath;
        private Set<String> indexTypes;
        private boolean preserveExisting = false;
        private int batchSize = 1000;
        private long sizeThreshold = 1_000_000;

        public Builder indexRootPath(Path path) {
            this.indexRootPath = path;
            return this;
        }

        public Builder stopwordsPath(Path path) {
            this.stopwordsPath = path;
            return this;
        }

        public Builder indexTypes(Set<String> types) {
            this.indexTypes = types;
            return this;
        }

        public Builder preserveExisting(boolean preserve) {
            this.preserveExisting = preserve;
            return this;
        }

        public Builder batchSize(int size) {
            this.batchSize = size;
            return this;
        }

        public Builder sizeThreshold(long threshold) {
            this.sizeThreshold = threshold;
            return this;
        }

        public IndexConfig build() {
            if (indexRootPath == null) {
                throw new IllegalStateException("Index root path must be specified");
            }
            if (stopwordsPath == null) {
                throw new IllegalStateException("Stopwords path must be specified");
            }
            if (indexTypes == null || indexTypes.isEmpty()) {
                throw new IllegalStateException("At least one index type must be specified");
            }
            return new IndexConfig(this);
        }
    }
} 