package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import com.google.common.collect.ListMultimap;
import java.io.*;
import java.util.*;
import java.sql.*;
import java.util.concurrent.*;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.ProgressTracker;

/**
 * Abstract base class that provides the framework for generating specialized indexes from annotated text.
 * Manages the connection between SQLite (source annotations) and LevelDB (target indexes), handling
 * batch processing, stopword filtering, and resource management. Implements efficient batch processing
 * with configurable sizes and memory management. Subclasses implement specific indexing strategies
 * while inheriting common functionality like stopword handling and database interactions. Includes
 * built-in progress tracking and performance optimization through configurable cache and buffer sizes.
 * @param <T> The type of index entry this generator processes
 */
public abstract class BaseIndexGenerator<T extends IndexEntry> implements AutoCloseable {
    protected static final Logger logger = LoggerFactory.getLogger(BaseIndexGenerator.class);
    public static final String DELIMITER = "\0";
    // Use ASCII unit separator (0x1F) as it's unlikely to appear in natural text
    public static final char ESCAPE_CHAR = '\u001F';
    public static final String ESCAPED_NULL = ESCAPE_CHAR + "0" + ESCAPE_CHAR;
    protected final DB levelDb;
    protected final Set<String> stopwords;
    protected final int batchSize;
    protected final Connection sqliteConn;
    protected final String tableName;
    private final ExecutorService executorService;
    private final int threadCount;
    private final Path tempDir;
    protected long totalEntries = 0;
    protected final ProgressTracker progress;
    private final String levelDbPath;

    protected BaseIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, String tableName) throws IOException {
        this(levelDbPath, stopwordsPath, batchSize, sqliteConn, tableName, 
             Runtime.getRuntime().availableProcessors(), new ProgressTracker());
    }

    protected BaseIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, String tableName, int threadCount) throws IOException {
        this(levelDbPath, stopwordsPath, batchSize, sqliteConn, tableName, threadCount, new ProgressTracker());
    }

    protected BaseIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, String tableName, int threadCount, ProgressTracker progress) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        options.writeBufferSize(64 * 1024 * 1024); // 64MB write buffer
        options.cacheSize(512 * 1024 * 1024L); // 512MB cache

        this.levelDb = factory.open(new File(levelDbPath), options);
        this.stopwords = loadStopwords(stopwordsPath);
        this.batchSize = batchSize;
        this.sqliteConn = sqliteConn;
        this.tableName = tableName;
        this.threadCount = Math.max(1, threadCount);
        this.executorService = Executors.newFixedThreadPool(this.threadCount);
        this.tempDir = Files.createTempDirectory("index-");
        this.progress = progress;
        this.levelDbPath = levelDbPath;
        
        // Ensure temp directory cleanup on JVM shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.walk(tempDir)
                     .sorted((a, b) -> b.compareTo(a)) // Reverse order to delete contents first
                     .forEach(path -> {
                         try {
                             Files.deleteIfExists(path);
                         } catch (IOException e) {
                             logger.error("Failed to delete temporary file: " + path, e);
                         }
                     });
            } catch (IOException e) {
                logger.error("Failed to cleanup temporary directory", e);
            }
        }));
    }

    private Set<String> loadStopwords(String path) throws IOException {
        Set<String> words = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                words.add(line.trim().toLowerCase());
            }
        }
        return words;
    }

    protected boolean isStopword(String word) {
        return word == null || stopwords.contains(word.toLowerCase());
    }

    /**
     * Escapes null bytes in text by replacing them with a reversible escape sequence.
     * This prevents conflicts with our delimiter while preserving the original meaning.
     * The escape sequence uses ASCII unit separator (0x1F) as it's unlikely to appear in natural text.
     * 
     * @param text The text to sanitize
     * @return The sanitized text with null bytes escaped
     */
    protected static String sanitizeText(String text) {
        if (text == null) {
            return null;
        }
        // Replace null bytes with our escape sequence
        return text.replace(DELIMITER, ESCAPED_NULL).trim();
    }

    /**
     * Reverses the null byte escaping done by sanitizeText.
     * 
     * @param text The text with escaped null bytes
     * @return The original text with null bytes restored
     */
    protected static String desanitizeText(String text) {
        if (text == null) {
            return null;
        }
        // Replace our escape sequence with actual null bytes
        return text.replace(ESCAPED_NULL, DELIMITER);
    }

    /**
     * Fetches a batch of entries from the SQLite database.
     * @param offset The offset to start fetching from
     * @return A list of index entries
     * @throws SQLException if there is an error accessing the database
     */
    protected abstract List<T> fetchBatch(int offset) throws SQLException;

    /**
     * Process a single partition of entries.
     * @param partition The partition of entries to process
     * @return A multimap of keys to position lists
     * @throws IOException if there is an error writing to LevelDB
     */
    protected abstract ListMultimap<String, PositionList> processPartition(List<T> partition) throws IOException;

    /**
     * Partitions the input entries into batches for parallel processing.
     * Creates one partition per thread to enable parallel processing.
     * Ensures that entries from the same document are kept together
     * to maintain consistency and prevent overlapping entries.
     * @param entries The entries to partition
     * @return A list of partitions
     */
    protected List<List<T>> partitionEntries(List<T> entries) {
        if (entries == null || entries.isEmpty()) {
            return new ArrayList<>();
        }

        // For single thread, return a single partition
        if (threadCount == 1) {
            List<List<T>> partitions = new ArrayList<>();
            partitions.add(new ArrayList<>(entries));
            return partitions;
        }

        // Group entries by document ID
        Map<Integer, List<T>> documentGroups = new HashMap<>();
        for (T entry : entries) {
            documentGroups.computeIfAbsent(entry.getDocumentId(), k -> new ArrayList<>()).add(entry);
        }

        // Sort documents by ID
        List<Integer> sortedDocIds = new ArrayList<>(documentGroups.keySet());
        Collections.sort(sortedDocIds);

        // For small inputs, create one partition per document (up to thread count)
        if (entries.size() < 1000) {
            List<List<T>> partitions = new ArrayList<>();
            List<T> currentPartition = new ArrayList<>();
            int currentDocCount = 0;
            int targetDocsPerPartition = Math.max(1, sortedDocIds.size() / threadCount);

            for (Integer docId : sortedDocIds) {
                currentPartition.addAll(documentGroups.get(docId));
                currentDocCount++;

                if (currentDocCount >= targetDocsPerPartition && partitions.size() < threadCount - 1) {
                    partitions.add(currentPartition);
                    currentPartition = new ArrayList<>();
                    currentDocCount = 0;
                }
            }

            if (!currentPartition.isEmpty()) {
                partitions.add(currentPartition);
            }

            return partitions;
        }

        // For larger inputs, partition based on size
        // Calculate target size for each partition
        int targetSize = entries.size() / threadCount;

        // Create partitions
        List<List<T>> partitions = new ArrayList<>();
        List<T> currentPartition = new ArrayList<>();
        int currentSize = 0;

        // Distribute documents to partitions
        for (Integer docId : sortedDocIds) {
            List<T> docEntries = documentGroups.get(docId);
            
            // If adding this document would exceed target size and we have enough for a partition,
            // start a new partition (unless this is the last possible partition)
            if (currentSize > 0 && currentSize + docEntries.size() > targetSize * 1.5 && 
                partitions.size() < threadCount - 1) {
                partitions.add(currentPartition);
                currentPartition = new ArrayList<>();
                currentSize = 0;
            }
            
            // Add all entries for this document to current partition
            currentPartition.addAll(docEntries);
            currentSize += docEntries.size();
        }

        // Add the last partition if it's not empty
        if (!currentPartition.isEmpty()) {
            partitions.add(currentPartition);
        }

        return partitions;
    }

    /**
     * Checks if a key exists in the LevelDB database.
     * @param key The key to check for
     * @return true if the key exists, false otherwise
     */
    public boolean hasKey(String key) {
        try {
            byte[] value = levelDb.get(bytes(key));
            return value != null;
        } catch (DBException e) {
            logger.error("Error checking key existence: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Writes a value to LevelDB with the given key.
     * @param key The key to write
     * @param value The value to write
     * @throws IOException if there is an error writing to LevelDB
     */
    protected void writeToLevelDb(String key, byte[] value) throws IOException {
        levelDb.put(bytes(key), value);
    }

    /**
     * Processes entries in parallel using the thread pool.
     */
    protected void processBatch(List<T> entries) throws IOException {
        try {
            // For small batches or single thread, process directly
            if (entries.size() < 10000 || threadCount == 1) {
                ListMultimap<String, PositionList> result = processPartition(entries);
                for (String key : result.keySet()) {
                    List<PositionList> positions = result.get(key);
                    if (!positions.isEmpty()) {
                        // First try to read existing positions from LevelDB
                        byte[] existingData = levelDb.get(bytes(key));
                        PositionList merged;
                        if (existingData != null) {
                            merged = PositionList.deserialize(existingData);
                        } else {
                            merged = positions.get(0);
                        }
                        
                        // Merge all positions
                        for (int i = (existingData != null ? 0 : 1); i < positions.size(); i++) {
                            merged.merge(positions.get(i));
                        }
                        writeToLevelDb(key, merged.serialize());
                    }
                }
                return;
            }

            // Process in parallel
            List<Future<ListMultimap<String, PositionList>>> futures = new ArrayList<>();

            // Submit tasks
            for (int i = 0; i < threadCount; i++) {
                int start = i * entries.size() / threadCount;
                int end = (i + 1) * entries.size() / threadCount;
                List<T> partition = entries.subList(start, end);
                futures.add(executorService.submit(new IndexGenerationTask(partition)));
            }

            // Collect all results before merging to reduce I/O overhead
            Map<String, List<PositionList>> mergedResults = new HashMap<>();
            
            for (Future<ListMultimap<String, PositionList>> future : futures) {
                ListMultimap<String, PositionList> partialResult = future.get(1, TimeUnit.HOURS);
                
                // Merge results in memory
                for (String key : partialResult.keySet()) {
                    List<PositionList> positions = partialResult.get(key);
                    if (!positions.isEmpty()) {
                        mergedResults.computeIfAbsent(key, k -> new ArrayList<>()).addAll(positions);
                    }
                }
            }
            
            // Write all results to LevelDB in one go
            for (Map.Entry<String, List<PositionList>> entry : mergedResults.entrySet()) {
                String key = entry.getKey();
                List<PositionList> positions = entry.getValue();
                if (!positions.isEmpty()) {
                    // First try to read existing positions from LevelDB
                    byte[] existingData = levelDb.get(bytes(key));
                    PositionList merged;
                    if (existingData != null) {
                        merged = PositionList.deserialize(existingData);
                    } else {
                        merged = positions.get(0);
                    }
                    
                    // Merge all positions
                    for (int i = (existingData != null ? 0 : 1); i < positions.size(); i++) {
                        merged.merge(positions.get(i));
                    }
                    writeToLevelDb(key, merged.serialize());
                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to process entries in parallel", e);
        }
    }

    /**
     * Task for processing a partition of entries.
     */
    private class IndexGenerationTask implements Callable<ListMultimap<String, PositionList>> {
        private final List<T> partition;

        IndexGenerationTask(List<T> partition) {
            this.partition = partition;
        }

        @Override
        public ListMultimap<String, PositionList> call() throws IOException {
            return processPartition(partition);
        }
    }

    /**
     * Generates the index by processing all entries in batches.
     * @throws SQLException if there is an error accessing the database
     * @throws IOException if there is an error writing to LevelDB
     */
    public void generateIndex() throws SQLException, IOException {
        // Count total entries
        try (Statement stmt = sqliteConn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
            if (rs.next()) {
                totalEntries = rs.getLong(1);
            }
        }

        // Process entries in batches
        int offset = 0;
        String indexType = getClass().getSimpleName().replace("IndexGenerator", "").toLowerCase();
        progress.startIndex("entries", totalEntries);

        while (true) {
            List<T> entries = fetchBatch(offset);
            if (entries.isEmpty()) {
                break;
            }
            
            processBatch(entries);
            offset += entries.size();
            progress.updateIndex(entries.size());
        }

        progress.completeIndex();
    }

    /**
     * Gets the path to the LevelDB index directory
     * @return The path to the LevelDB index directory
     */
    public String getIndexPath() {
        return levelDbPath;
    }

    @Override
    public void close() throws IOException {
        progress.close();
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
            levelDb.close();
    }
}
