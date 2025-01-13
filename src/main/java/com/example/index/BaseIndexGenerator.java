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
import me.tongfei.progressbar.*;
import java.util.stream.*;

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
    private long totalEntries = 0;

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

    protected List<IndexEntry> fetchBatch(long offset) throws SQLException {
        List<IndexEntry> entries = new ArrayList<>();
        // First get the document IDs for this batch
        String docQuery = String.format("""
                SELECT DISTINCT d.document_id, d.timestamp
                FROM documents d
                JOIN annotations a ON d.document_id = a.document_id
                ORDER BY d.document_id
                LIMIT %d OFFSET %d
            """, batchSize, offset);
        
        try (Statement stmt = sqliteConn.createStatement()) {
            // Get document IDs for this batch
            List<DocumentInfo> documents = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(docQuery)) {
                while (rs.next()) {
                    documents.add(new DocumentInfo(
                        rs.getInt("document_id"),
                        ZonedDateTime.parse(rs.getString("timestamp")).toLocalDate()
                    ));
                }
            }
            
            // Now fetch all annotations for these documents
            if (!documents.isEmpty()) {
                String ids = documents.stream()
                    .map(d -> String.valueOf(d.documentId))
                    .collect(Collectors.joining(","));
                
                String annotationQuery = String.format("""
                        SELECT document_id, sentence_id, begin_char, end_char,
                               lemma, pos
                        FROM annotations
                        WHERE document_id IN (%s)
                        ORDER BY document_id, sentence_id, begin_char
                    """, ids);
                
                try (ResultSet rs = stmt.executeQuery(annotationQuery)) {
                    while (rs.next()) {
                        int docId = rs.getInt("document_id");
                        LocalDate timestamp = documents.stream()
                            .filter(d -> d.documentId == docId)
                            .findFirst()
                            .map(d -> d.timestamp)
                            .orElseThrow();
                        
                        entries.add(new IndexEntry(
                            docId,
                            rs.getInt("sentence_id"),
                            rs.getInt("begin_char"),
                            rs.getInt("end_char"),
                            rs.getString("lemma"),
                            rs.getString("pos"),
                            timestamp));
                    }
                }
            }
        }
        return entries;
    }

    private static class DocumentInfo {
        final int documentId;
        final LocalDate timestamp;
        
        DocumentInfo(int documentId, LocalDate timestamp) {
            this.documentId = documentId;
            this.timestamp = timestamp;
        }
    }

    protected void writeToLevelDb(String key, byte[] value) throws IOException {
        levelDb.put(bytes(key), value);
    }

    /**
     * Partitions the input entries into batches for parallel processing.
     * Partitions are created along document boundaries to ensure no document
     * is split across partitions.
     */
    protected List<List<IndexEntry>> partitionEntries(List<IndexEntry> entries) {
        if (entries.isEmpty()) {
            return Collections.emptyList();
        }

        // Group entries by document ID
        Map<Integer, List<IndexEntry>> entriesByDoc = new HashMap<>();
        for (IndexEntry entry : entries) {
            entriesByDoc.computeIfAbsent(entry.documentId, k -> new ArrayList<>()).add(entry);
        }

        // Calculate optimal number of documents per partition
        int totalDocs = entriesByDoc.size();
        int optimalThreadCount = Math.min(threadCount, Math.max(2, totalDocs));
        int docsPerPartition = Math.max(1, totalDocs / optimalThreadCount);

        List<List<IndexEntry>> partitions = new ArrayList<>();
        List<IndexEntry> currentPartition = new ArrayList<>();
        int currentDocCount = 0;

        // Create partitions by document
        for (List<IndexEntry> docEntries : entriesByDoc.values()) {
            currentPartition.addAll(docEntries);
            currentDocCount++;

            if (currentDocCount >= docsPerPartition) {
                partitions.add(currentPartition);
                currentPartition = new ArrayList<>();
                currentDocCount = 0;
            }
        }

        // Add any remaining entries as the last partition
        if (!currentPartition.isEmpty()) {
            partitions.add(currentPartition);
        }

        // Log partition information
        if (logger.isDebugEnabled()) {
            for (int i = 0; i < partitions.size(); i++) {
                List<IndexEntry> partition = partitions.get(i);
                Set<Integer> docsInPartition = partition.stream()
                    .map(e -> e.documentId)
                    .collect(Collectors.toSet());
                logger.debug("Partition {} contains {} entries from {} documents", 
                    i, partition.size(), docsInPartition.size());
            }
        }

        return partitions;
    }

    private int countOverlappingEntries(List<IndexEntry> partition1, List<IndexEntry> partition2) {
        Set<String> entries1 = partition1.stream()
            .map(e -> String.format("%d-%d-%d-%d", e.documentId, e.sentenceId, e.beginChar, e.endChar))
            .collect(Collectors.toSet());
        
        return (int) partition2.stream()
            .map(e -> String.format("%d-%d-%d-%d", e.documentId, e.sentenceId, e.beginChar, e.endChar))
            .filter(entries1::contains)
            .count();
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

    public void generateIndex() throws IOException {
        long startTime = System.currentTimeMillis();
        
        // Count total entries first
        try (Statement stmt = sqliteConn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM annotations");
            if (rs.next()) {
                totalEntries = rs.getLong("count");
            }
        } catch (SQLException e) {
            logger.warn("Could not get total entry count", e);
            totalEntries = -1;
        }

        System.out.printf("Generating %s index using %d threads...\n", 
            getClass().getSimpleName().replace("IndexGenerator", "").toLowerCase(),
            threadCount);

        long processedEntries = 0;
        try (ProgressBar pb = new ProgressBar("Processing entries", totalEntries > 0 ? totalEntries : 100000)) {
            List<IndexEntry> batch = new ArrayList<>();
            
            while (true) {
                List<IndexEntry> entries;
                try {
                    entries = fetchBatch(processedEntries);
                } catch (SQLException e) {
                    throw new IOException("Error fetching batch", e);
                }
                if (entries.isEmpty()) {
                    break;
                }
                
                batch.addAll(entries);
                if (batch.size() >= batchSize) {
                    processBatch(batch);
                    processedEntries += batch.size();
                    pb.stepBy(batch.size());
                    batch.clear();
                }
            }
            
            // Process remaining entries
            if (!batch.isEmpty()) {
                processBatch(batch);
                processedEntries += batch.size();
                pb.stepBy(batch.size());
            }
        }

        // Final merge if needed
        if (tempDir != null && Files.exists(tempDir)) {
            List<Path> tempFiles = Files.list(tempDir)
                .filter(p -> p.toString().endsWith(".gz"))
                .collect(Collectors.toList());
            
            if (!tempFiles.isEmpty()) {
                System.out.println("Merging temporary files...");
                try (ProgressBar pb = new ProgressBar("Merging", tempFiles.size())) {
                    DiskBasedMerger merger = new DiskBasedMerger(tempDir);
                    merger.mergeIndexes(tempFiles, tempDir.resolve("final.gz"));
                    pb.stepTo(pb.getMax());
                }
            }
        }

        double timeInSeconds = (System.currentTimeMillis() - startTime) / 1000.0;
        System.out.printf("Index generation complete. Processed %d entries in %.2f seconds\n", 
            processedEntries, timeInSeconds);
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
