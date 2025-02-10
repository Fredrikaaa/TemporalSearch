package com.example.index;

import com.google.common.collect.ListMultimap;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.ProgressTracker;
import com.example.logging.IndexingMetrics;
import com.google.code.externalsorting.ExternalSort;
import com.example.core.Position;
import com.example.core.PositionList;

import java.io.*;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.*;
import java.nio.charset.Charset;

/**
 * Abstract base class for streaming index generation that processes large datasets efficiently
 * while maintaining bounded memory usage. Uses external sorting for scalable processing.
 *
 * @param <T> The type of index entry this generator processes
 */
public abstract class IndexGenerator<T extends IndexEntry> implements AutoCloseable {
    protected static final Logger logger = LoggerFactory.getLogger(IndexGenerator.class);
    public static final String DELIMITER = "\0";
    public static final char ESCAPE_CHAR = '\u001F';

    private final DB levelDb;
    private final Set<String> stopwords;
    protected final Connection sqliteConn;
    protected final ProgressTracker progress;
    private final Path tempDir;
    private long totalNGramsGenerated = 0;
    protected final IndexConfig config;

    // Memory and processing configuration
    protected static final int DOC_BATCH_SIZE = 1000; // Smaller batch size for more frequent progress updates

    /**
     * Gets the name of the table to query for entries.
     * @return The table name
     */
    protected abstract String getTableName();

    /**
     * Gets the name of this index for progress tracking and logging.
     * @return The name of the index
     */
    protected abstract String getIndexName();

    /**
     * Converts a string to UTF-8 bytes for LevelDB operations.
     * @param str The string to convert
     * @return The UTF-8 encoded bytes
     */
    protected static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    protected IndexGenerator(String levelDbPath, String stopwordsPath,
            Connection sqliteConn, ProgressTracker progress) throws IOException {
        this(levelDbPath, stopwordsPath, sqliteConn, progress, new IndexConfig.Builder().build());
    }

    protected IndexGenerator(String levelDbPath, String stopwordsPath,
            Connection sqliteConn, ProgressTracker progress, IndexConfig config) throws IOException {
        // Initialize LevelDB with performance options
        this.levelDb = factory.open(new File(levelDbPath), LevelDBConfig.createOptimizedOptions());

        this.stopwords = loadStopwords(stopwordsPath);
        this.sqliteConn = sqliteConn;
        this.progress = progress;
        this.tempDir = Files.createTempDirectory("index-");
        this.config = config;

        // Register shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (Files.exists(tempDir)) {
                    Files.walk(tempDir)
                         .sorted((a, b) -> b.compareTo(a))
                         .forEach(path -> {
                             try {
                                 Files.deleteIfExists(path);
                             } catch (IOException e) {
                                 logger.debug("Could not delete temporary file: {} ({})", path, e.getMessage());
                             }
                         });
                } else {
                    logger.debug("Temporary directory already cleaned up: {}", tempDir);
                }
            } catch (IOException e) {
                logger.debug("Failed to cleanup temporary directory: {} ({})", tempDir, e.getMessage());
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
     * Fetches a batch of entries from the database for processing.
     * @param offset The offset to start fetching from
     * @return List of entries for processing
     */
    protected abstract List<T> fetchBatch(int offset) throws SQLException;

    /**
     * Process a batch of documents and return a map of terms to their position lists.
     * @param batch The batch of documents to process
     * @return A multimap of terms to their position lists
     */
    protected abstract ListMultimap<String, PositionList> processBatch(List<T> batch) throws IOException;

    /**
     * Writes a batch of processed entries to a temporary file.
     * @param positions The processed position lists to write
     * @return The temporary file containing the sorted entries
     */
    protected File writeBatchToTempFile(ListMultimap<String, PositionList> positions) throws IOException {
        File tempFile = Files.createTempFile(tempDir, "batch-", ".tmp").toFile();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            for (Map.Entry<String, Collection<PositionList>> entry : positions.asMap().entrySet()) {
                // Merge all position lists for this term
                PositionList mergedList = new PositionList();
                for (PositionList list : entry.getValue()) {
                    list.getPositions().forEach(mergedList::add);
                }
                
                String line = String.format("%s\t%s\n", 
                    entry.getKey(), 
                    Base64.getEncoder().encodeToString(mergedList.serialize()));
                writer.write(line);
            }
        }
        return tempFile;
    }

    /**
     * Writes the final merged and sorted entries to LevelDB.
     * @param sortedFile The file containing the sorted entries
     */
    protected void writeToLevelDB(File sortedFile) throws IOException {
        WriteBatch batch = null;
        int batchCount = 0;
        long startTime = System.currentTimeMillis();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(sortedFile))) {
            batch = levelDb.createWriteBatch();
            
            String currentTerm = null;
            PositionList mergedPositions = null;
            String line;

            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t", 2);
                if (parts.length != 2) {
                    logger.warn("Invalid line format: {}", line);
                    continue;
                }

                String term = parts[0];
                byte[] positionData = Base64.getDecoder().decode(parts[1]);
                PositionList positions = PositionList.deserialize(positionData);

                // First term initialization
                if (currentTerm == null) {
                    currentTerm = term;
                    mergedPositions = positions;
                    continue;
                }

                // If we have a new term, write the current one
                if (!currentTerm.equals(term)) {
                    // Write current term
                    writeToBatch(batch, currentTerm, mergedPositions);
                    batchCount++;
                    
                    // Write batch if full
                    if (batchCount >= LevelDBConfig.BATCH_SIZE) {
                        levelDb.write(batch);
                        batch.close();
                        batch = levelDb.createWriteBatch();
                        batchCount = 0;
                        logger.debug("Wrote batch of {} entries", LevelDBConfig.BATCH_SIZE);
                        
                        // Collect stats periodically
                        if (totalNGramsGenerated % (LevelDBConfig.BATCH_SIZE * 10) == 0) {
                            LevelDBConfig.collectLevelDbStats(levelDb);
                            long elapsed = System.currentTimeMillis() - startTime;
                            logger.info("Write progress: {} terms, {} terms/sec", 
                                totalNGramsGenerated,
                                String.format("%.2f", totalNGramsGenerated * 1000.0 / elapsed));
                        }
                    }
                    
                    // Start new term
                    currentTerm = term;
                    mergedPositions = positions;
                } else {
                    // Merge positions for same term
                    positions.getPositions().forEach(mergedPositions::add);
                }
            }

            // Write the last term if we have one
            if (currentTerm != null && mergedPositions != null) {
                writeToBatch(batch, currentTerm, mergedPositions);
                batchCount++;
            }

            // Write final batch if not empty
            if (batchCount > 0) {
                levelDb.write(batch);
                logger.debug("Wrote final batch of {} entries", batchCount);
                LevelDBConfig.collectLevelDbStats(levelDb);
            }
        } finally {
            if (batch != null) {
                try {
                    batch.close();
                } catch (Exception e) {
                    logger.warn("Error closing batch: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Writes a term and its positions to a WriteBatch.
     */
    private void writeToBatch(WriteBatch batch, String term, PositionList positions) throws IOException {
        batch.put(bytes(term), positions.serialize());
        totalNGramsGenerated++;
    }

    /**
     * Generates the index by processing documents in batches, sorting externally,
     * and merging to the final index store.
     */
    public void generateIndex() throws SQLException, IOException {
        List<File> tempFiles = new ArrayList<>();
        int offset = 0;
        IndexingMetrics metrics = new IndexingMetrics();
        metrics.startBatch(DOC_BATCH_SIZE, getIndexName());

        try {
            // Process batches and write to temp files
            while (true) {
                List<T> batch = fetchBatch(offset);
                if (batch.isEmpty()) {
                    break;
                }

                // Process batch and write to temp file
                ListMultimap<String, PositionList> positions = processBatch(batch);
                File tempFile = writeBatchToTempFile(positions);
                tempFiles.add(tempFile);

                // Update progress and metrics
                offset += batch.size();
                metrics.recordBatchSuccess(batch.size());
                progress.updateIndex(batch.size());
            }

            // Sort and merge temp files
            File outputFile = new File(tempDir.toFile(), "sorted.tmp");
            ExternalSort.mergeSortedFiles(tempFiles, outputFile, new PositionListComparator());

            // Write sorted entries to LevelDB
            writeToLevelDB(outputFile);
            
            // Close LevelDB after writing
            try {
                levelDb.close();
            } catch (IOException e) {
                logger.warn("Error closing LevelDB after writing: {}", e.getMessage());
            }

            // Final metrics
            metrics.logIndexingMetrics();
            logger.info("Index generation complete. Total entries: {}", totalNGramsGenerated);

        } finally {
            // Cleanup temp files
            for (File file : tempFiles) {
                try {
                    Files.deleteIfExists(file.toPath());
                } catch (IOException e) {
                    logger.debug("Could not delete temp file: {} ({})", file, e.getMessage());
                }
            }
        }
    }

    /**
     * Comparator for sorting position list entries
     */
    private static class PositionListComparator implements Comparator<String> {
        @Override
        public int compare(String line1, String line2) {
            String term1 = line1.split("\t", 2)[0];
            String term2 = line2.split("\t", 2)[0];
            return term1.compareTo(term2);
        }
    }

    /**
     * Gets the total number of unique n-grams generated during indexing.
     * @return The total number of unique n-grams
     */
    public long getTotalNGramsGenerated() {
        return totalNGramsGenerated;
    }

    @Override
    public void close() throws IOException {
        if (levelDb != null) {
            levelDb.close();
        }
        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                 .sorted((a, b) -> b.compareTo(a))
                 .forEach(path -> {
                     try {
                         Files.deleteIfExists(path);
                     } catch (IOException e) {
                         logger.debug("Could not delete temporary file: {} ({})", path, e.getMessage());
                     }
                 });
        }
    }
} 
