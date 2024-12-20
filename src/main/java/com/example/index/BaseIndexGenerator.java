package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.*;
import java.util.*;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class that provides the framework for generating specialized indexes from annotated text.
 * Manages the connection between SQLite (source annotations) and LevelDB (target indexes), handling
 * batch processing, stopword filtering, and resource management. Implements efficient batch processing
 * with configurable sizes and memory management. Subclasses implement specific indexing strategies
 * while inheriting common functionality like stopword handling and database interactions. Includes
 * built-in progress tracking and performance optimization through configurable cache and buffer sizes.
 */
public abstract class BaseIndexGenerator implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BaseIndexGenerator.class);
    protected final DB levelDb;
    protected final Set<String> stopwords;
    protected final int batchSize;
    protected final Connection sqliteConn;
    protected final String tableName;
    private final ExecutorService executorService;
    private final int threadCount;
    private final Path tempDir;

    protected BaseIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, String tableName) throws IOException {
        this(levelDbPath, stopwordsPath, batchSize, sqliteConn, tableName, 
             Runtime.getRuntime().availableProcessors());
    }

    protected BaseIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, String tableName, int threadCount) throws IOException {
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

    protected List<IndexEntry> fetchBatch(int offset) throws SQLException {
        String query = """
                    SELECT a.document_id, a.sentence_id, a.begin_char, a.end_char,
                           a.lemma, a.pos, d.timestamp
                    FROM annotations a
                    JOIN documents d ON a.document_id = d.document_id
                    ORDER BY a.document_id, a.sentence_id, a.begin_char
                    LIMIT ? OFFSET ?
                """;

        List<IndexEntry> entries = new ArrayList<>();
        try (PreparedStatement stmt = sqliteConn.prepareStatement(query)) {
            stmt.setInt(1, batchSize);
            stmt.setInt(2, offset);
            stmt.setFetchSize(batchSize);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String timestamp = rs.getString("timestamp");
                    LocalDate date = ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_DATE_TIME)
                            .toLocalDate();

                    entries.add(new IndexEntry(
                            rs.getInt("document_id"),
                            rs.getInt("sentence_id"),
                            rs.getInt("begin_char"),
                            rs.getInt("end_char"),
                            rs.getString("lemma"),
                            rs.getString("pos"),
                            date));
                }
            }
        }
        return entries;
    }

    protected void writeToLevelDb(String key, byte[] value) throws IOException {
        levelDb.put(bytes(key), value);
    }

    /**
     * Partitions the input entries into batches for parallel processing.
     * Ensures that n-grams are not split across partitions by adding overlap.
     */
    protected List<List<IndexEntry>> partitionEntries(List<IndexEntry> entries) {
        // For testing with small inputs, ensure at least two partitions
        int minEntriesPerThread = entries.size() < 100 ? 1 : 10000;
        int optimalThreadCount = Math.min(threadCount, 
                                        Math.max(2, entries.size() / minEntriesPerThread));
        int partitionSize = Math.max(1, entries.size() / optimalThreadCount);
        
        List<List<IndexEntry>> partitions = new ArrayList<>();
        
        // Add overlap of 2 entries to ensure trigrams are not split
        int overlap = 2;
        
        for (int i = 0; i < entries.size(); i += partitionSize) {
            int start = Math.max(0, i - overlap);  // Include overlap from previous partition
            int end = Math.min(entries.size(), i + partitionSize + overlap);  // Include overlap for next partition
            
            // Only include entries from the same document and sentence at partition boundaries
            while (start > 0 && start < entries.size() && 
                   entries.get(start).documentId == entries.get(start - 1).documentId &&
                   entries.get(start).sentenceId == entries.get(start - 1).sentenceId) {
                start--;
            }
            
            while (end < entries.size() && 
                   entries.get(end - 1).documentId == entries.get(end).documentId &&
                   entries.get(end - 1).sentenceId == entries.get(end).sentenceId) {
                end++;
            }
            
            partitions.add(entries.subList(start, end));
        }
        return partitions;
    }

    /**
     * Process a single partition of entries.
     * To be implemented by concrete subclasses.
     */
    protected abstract ListMultimap<String, PositionList> processPartition(List<IndexEntry> partition) throws IOException;

    /**
     * Processes entries in parallel using the thread pool.
     */
    protected void processBatch(List<IndexEntry> entries) throws IOException {
        try {
            // For small batches or single thread, process directly
            if (entries.size() < 10000 || threadCount == 1) {
                ListMultimap<String, PositionList> result = processPartition(entries);
                for (String key : result.keySet()) {
                    List<PositionList> positions = result.get(key);
                    if (!positions.isEmpty()) {
                        PositionList merged = positions.get(0);
                        for (int i = 1; i < positions.size(); i++) {
                            merged.merge(positions.get(i));
                        }
                        writeToLevelDb(key, merged.serialize());
                    }
                }
                return;
            }

            // Use partitionEntries to maintain n-gram continuity
            List<List<IndexEntry>> partitions = partitionEntries(entries);
            List<Future<ListMultimap<String, PositionList>>> futures = new ArrayList<>();

            // Submit tasks
            for (List<IndexEntry> partition : partitions) {
                futures.add(executorService.submit(new IndexGenerationTask(partition)));
            }

            // Collect all results before merging to reduce I/O overhead
            Map<String, PositionList> mergedResults = new HashMap<>();
            
            for (Future<ListMultimap<String, PositionList>> future : futures) {
                ListMultimap<String, PositionList> partialResult = future.get(1, TimeUnit.HOURS);
                
                // Merge results in memory
                for (String key : partialResult.keySet()) {
                    List<PositionList> positions = partialResult.get(key);
                    if (!positions.isEmpty()) {
                        PositionList existing = mergedResults.get(key);
                        if (existing == null) {
                            mergedResults.put(key, positions.get(0));
                        } else {
                            existing.merge(positions.get(0));
                        }
                    }
                }
            }
            
            // Write all results to LevelDB in one go
            for (Map.Entry<String, PositionList> entry : mergedResults.entrySet()) {
                writeToLevelDb(entry.getKey(), entry.getValue().serialize());
            }
        } catch (NullPointerException e) {
            throw new IOException("Failed to process entries: encountered null entry", e);
        } catch (Exception e) {
            throw new IOException("Failed to process entries in parallel", e);
        }
    }

    /**
     * Task for processing a partition of entries.
     */
    private class IndexGenerationTask implements Callable<ListMultimap<String, PositionList>> {
        private final List<IndexEntry> partition;

        IndexGenerationTask(List<IndexEntry> partition) {
            this.partition = partition;
        }

        @Override
        public ListMultimap<String, PositionList> call() throws IOException {
            return processPartition(partition);
        }
    }

    public void generateIndex() throws SQLException, IOException {
        int offset = 0;
        int totalProcessed = 0;
        List<IndexEntry> batch;

        System.out.printf("Generating %s index using %d threads...%n", tableName, threadCount);
        long startTime = System.currentTimeMillis();

        while (!(batch = fetchBatch(offset)).isEmpty()) {
            processBatch(batch);
            offset += batchSize;
            totalProcessed += batch.size();

            if (totalProcessed % (batchSize * 10) == 0) {
                System.out.printf("Processed %d entries...%n", totalProcessed);
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("Index generation complete. Processed %d entries in %.2f seconds%n",
                totalProcessed, (endTime - startTime) / 1000.0);
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (levelDb != null) {
            levelDb.close();
        }
    }
}

/**
 * Data transfer object representing a single annotation entry from the SQLite database.
 * Encapsulates all relevant information about a token including its position, lemmatized form,
 * part-of-speech tag, and temporal information.
 */
class IndexEntry {
    final int documentId;
    final int sentenceId;
    final int beginChar;
    final int endChar;
    final String lemma;
    final String pos;
    final LocalDate timestamp;

    IndexEntry(int documentId, int sentenceId, int beginChar, int endChar,
            String lemma, String pos, LocalDate timestamp) {
        this.documentId = documentId;
        this.sentenceId = sentenceId;
        this.beginChar = beginChar;
        this.endChar = endChar;
        this.lemma = lemma;
        this.pos = pos;
        this.timestamp = timestamp;
    }
}
