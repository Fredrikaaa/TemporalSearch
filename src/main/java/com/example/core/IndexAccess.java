package com.example.core;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Core class for accessing LevelDB-based indexes.
 * Provides unified access for both read and write operations.
 */
public class IndexAccess implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IndexAccess.class);

    // Delimiter constants for different index types
    public static final char NGRAM_DELIMITER = '\0';  // Null byte delimiter used for n-grams (bigrams, trigrams)
    public static final String FIELD_DELIMITER = ":"; // Field delimiter used for structured data (NER, POS, dependencies)
    
    private final DB db;
    private final String indexPath;
    private final String indexType;
    private final AtomicBoolean isOpen;

    /**
     * Creates a new IndexAccess instance for a specific index type.
     *
     * @param indexPath Full path to the index directory
     * @param indexType The type of index (e.g., "unigram", "bigram", "dependency")
     * @param options LevelDB options for this index
     * @throws IndexAccessException if initialization fails
     */
    public IndexAccess(Path indexPath, String indexType, Options options) throws IndexAccessException {
        this.indexType = indexType;
        this.indexPath = indexPath.toString();
        this.isOpen = new AtomicBoolean(true);

        try {
            // Create index directory if it doesn't exist
            File indexDir = new File(indexPath.toString());
            if (!indexDir.exists()) {
                if (!indexDir.mkdirs()) {
                    throw new IndexAccessException(
                        "Failed to create index directory: " + indexPath,
                        indexType,
                        IndexAccessException.ErrorType.INITIALIZATION_ERROR
                    );
                }
            }

            // Initialize LevelDB
            this.db = factory.open(indexDir, options);
            logger.info("Initialized IndexAccess for type {} at {}", indexType, indexPath);
            
        } catch (IOException e) {
            throw new IndexAccessException(
                "Failed to initialize index: " + e.getMessage(),
                indexType,
                IndexAccessException.ErrorType.INITIALIZATION_ERROR,
                e
            );
        }
    }

    /**
     * Stores a position list for a given key.
     */
    public void put(byte[] key, PositionList positions) throws IndexAccessException {
        checkOpen();
        try {
            byte[] existing = db.get(key);
            if (existing != null) {
                PositionList existingPositions = PositionList.deserialize(existing);
                positions.merge(existingPositions);
            }
            
            db.put(key, positions.serialize());
        } catch (Exception e) {
            throw new IndexAccessException(
                "Failed to put entry: " + e.getMessage(),
                indexType,
                IndexAccessException.ErrorType.WRITE_ERROR,
                e
            );
        }
    }

    /**
     * Writes a batch of operations atomically.
     */
    public void writeBatch(WriteBatch batch) throws IndexAccessException {
        checkOpen();
        try {
            db.write(batch);
        } catch (Exception e) {
            throw new IndexAccessException(
                "Failed to write batch: " + e.getMessage(),
                indexType,
                IndexAccessException.ErrorType.WRITE_ERROR,
                e
            );
        }
    }

    /**
     * Creates a new write batch.
     */
    public WriteBatch createBatch() {
        return db.createWriteBatch();
    }

    /**
     * Retrieves positions for a given key.
     */
    public Optional<PositionList> get(byte[] key) throws IndexAccessException {
        checkOpen();
        try {
            byte[] value = db.get(key);
            if (value == null) {
                return Optional.empty();
            }
            return Optional.of(PositionList.deserialize(value));
        } catch (Exception e) {
            throw new IndexAccessException(
                "Failed to get entry: " + e.getMessage(),
                indexType,
                IndexAccessException.ErrorType.READ_ERROR,
                e
            );
        }
    }

    /**
     * Creates a new iterator over the database.
     * The caller is responsible for closing the iterator.
     */
    public DBIterator iterator() throws IndexAccessException {
        checkOpen();
        return db.iterator();
    }

    /**
     * Gets the type of this index.
     */
    public String getIndexType() {
        return indexType;
    }

    /**
     * Checks if the index is still open.
     */
    public boolean isOpen() {
        return isOpen.get();
    }

    private void checkOpen() throws IndexAccessException {
        if (!isOpen.get()) {
            throw new IndexAccessException(
                "Index is closed",
                indexType,
                IndexAccessException.ErrorType.RESOURCE_ERROR
            );
        }
    }

    @Override
    public void close() throws IndexAccessException {
        if (isOpen.compareAndSet(true, false)) {
            try {
                db.close();
            } catch (IOException e) {
                throw new IndexAccessException(
                    "Failed to close index: " + e.getMessage(),
                    indexType,
                    IndexAccessException.ErrorType.RESOURCE_ERROR,
                    e
                );
            }
        }
    }

    // Utility methods
    protected static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    protected static String asString(byte[] bytes) {
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Gets the document text for a given document ID.
     * 
     * @param documentId The document ID
     * @return The document text, or null if not found
     */
    public String getDocumentText(int documentId) {
        try {
            checkOpen();
            // In a real implementation, you would retrieve the document text from the index
            // For now, we'll just return a placeholder
            return "This is the text of document " + documentId + ". It contains multiple sentences. This is the second sentence.";
        } catch (Exception e) {
            logger.error("Failed to get document text: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Gets the sentences for a given document ID.
     * 
     * @param documentId The document ID
     * @return Array of sentences, or null if not found
     */
    public String[] getDocumentSentences(int documentId) {
        try {
            checkOpen();
            // In a real implementation, you would retrieve the sentences from the index
            // For now, we'll just return placeholders
            return new String[] {
                "This is the text of document " + documentId + ".",
                "It contains multiple sentences.",
                "This is the third sentence."
            };
        } catch (Exception e) {
            logger.error("Failed to get document sentences: {}", e.getMessage(), e);
            return null;
        }
    }
} 