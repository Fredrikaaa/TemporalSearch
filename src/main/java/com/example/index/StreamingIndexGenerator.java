package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.base.Preconditions;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.ProgressTracker;
import com.google.code.externalsorting.ExternalSort;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.*;

// Add imports for Position and PositionList
import com.example.index.Position;
import com.example.index.PositionList;

/**
 * Abstract base class for streaming index generation that processes large datasets efficiently
 * while maintaining bounded memory usage. Uses external sorting for scalable processing.
 *
 * @param <T> The type of index entry this generator processes
 */
public abstract class StreamingIndexGenerator<T extends IndexEntry> implements AutoCloseable {
    protected static final Logger logger = LoggerFactory.getLogger(StreamingIndexGenerator.class);
    public static final String DELIMITER = "\0";
    public static final char ESCAPE_CHAR = '\u001F';

    private final DB levelDb;
    private final Set<String> stopwords;
    protected final Connection sqliteConn;
    private final ProgressTracker progress;
    private final Path tempDir;
    private long totalNGramsGenerated = 0;

    // Memory and processing configuration
    private static final int MAX_MEMORY_MB = 512;
    private static final int BUFFER_SIZE = 8 * 1024 * 1024; // 8MB
    protected static final int DOC_BATCH_SIZE = 1000;

    /**
     * Gets the name of the table to query for entries.
     * @return The table name
     */
    protected String getTableName() {
        return "annotations"; // Default to annotations table, subclasses can override
    }

    /**
     * Converts a string to UTF-8 bytes for LevelDB operations.
     * @param str The string to convert
     * @return The UTF-8 encoded bytes
     */
    protected static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    protected StreamingIndexGenerator(String levelDbPath, String stopwordsPath,
            Connection sqliteConn, ProgressTracker progress) throws IOException {
        // Initialize LevelDB with performance options
        Options options = new Options();
        options.createIfMissing(true);
        options.writeBufferSize(64 * 1024 * 1024); // 64MB write buffer
        options.cacheSize(512 * 1024 * 1024L); // 512MB cache
        this.levelDb = factory.open(new File(levelDbPath), options);

        this.stopwords = loadStopwords(stopwordsPath);
        this.sqliteConn = sqliteConn;
        this.progress = progress;
        this.tempDir = Files.createTempDirectory("streaming-index-");

        // Register shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.walk(tempDir)
                     .sorted((a, b) -> b.compareTo(a))
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
    protected File writeBatchToTempFile(Map<String, PositionList> positions) throws IOException {
        File tempFile = Files.createTempFile(tempDir, "batch-", ".tmp").toFile();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            for (Map.Entry<String, PositionList> entry : positions.entrySet()) {
                String line = String.format("%s\t%s\n", 
                    entry.getKey(), 
                    Base64.getEncoder().encodeToString(entry.getValue().serialize()));
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
        try (BufferedReader reader = new BufferedReader(new FileReader(sortedFile))) {
            batch = levelDb.createWriteBatch();
            
            String currentTerm = null;
            PositionList mergedPositions = null;
            int batchCount = 0;
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

                if (currentTerm == null) {
                    currentTerm = term;
                    mergedPositions = positions;
                    totalNGramsGenerated++;
                } else if (!term.equals(currentTerm)) {
                    // Write the previous term's merged positions
                    batch.put(bytes(currentTerm), mergedPositions.serialize());
                    batchCount++;

                    // Reset for new term
                    currentTerm = term;
                    mergedPositions = positions;
                    totalNGramsGenerated++;

                    // Write batch if it's full
                    if (batchCount >= 1000) {
                        levelDb.write(batch);
                        batch.close();
                        batch = levelDb.createWriteBatch();
                        batchCount = 0;
                    }
                } else {
                    // Merge positions for the same term
                    positions.getPositions().forEach(mergedPositions::add);
                }
            }

            // Write the last term if exists
            if (currentTerm != null) {
                batch.put(bytes(currentTerm), mergedPositions.serialize());
            }

            // Write final batch
            if (batchCount > 0) {
                levelDb.write(batch);
            }
        } finally {
            if (batch != null) {
                batch.close();
            }
        }
    }

    /**
     * Generates the index by processing documents in batches, sorting externally,
     * and merging to the final index store.
     */
    public void generateIndex() throws SQLException, IOException {
        List<File> tempFiles = new ArrayList<>();
        int offset = 0;
        long totalProcessed = 0;

        // Get total count of entries
        try (Statement stmt = sqliteConn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + getTableName());
            if (rs.next()) {
                long totalEntries = rs.getLong(1);
                progress.startIndex("entries", totalEntries);
            }
        }

        try {
            // Process documents in batches
            while (true) {
                List<T> batch = fetchBatch(offset);
                if (batch.isEmpty()) {
                    break;
                }

                ListMultimap<String, PositionList> batchPositions = processBatch(batch);
                Map<String, PositionList> mergedPositions = new HashMap<>();

                // Merge position lists for the same term within the batch
                for (Map.Entry<String, PositionList> entry : batchPositions.entries()) {
                    mergedPositions.merge(
                        entry.getKey(),
                        entry.getValue(),
                        (existing, newList) -> {
                            newList.getPositions().forEach(existing::add);
                            return existing;
                        }
                    );
                }

                File tempFile = writeBatchToTempFile(mergedPositions);
                tempFiles.add(tempFile);

                totalProcessed += batch.size();
                progress.updateIndex(batch.size());
                offset += batch.size();
            }

            // Merge sorted files
            if (!tempFiles.isEmpty()) {
                File outputFile = new File(tempDir.toFile(), "sorted.tmp");
                ExternalSort.mergeSortedFiles(tempFiles, outputFile, new PositionListComparator());

                // Write merged results to LevelDB
                writeToLevelDB(outputFile);
                outputFile.delete();
            }

            // Mark progress as complete
            progress.completeIndex();

        } finally {
            // Cleanup temporary files
            tempFiles.forEach(File::delete);
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
        // Cleanup temp directory
        if (tempDir != null) {
            Files.walk(tempDir)
                 .sorted((a, b) -> b.compareTo(a))
                 .forEach(path -> {
                     try {
                         Files.deleteIfExists(path);
                     } catch (IOException e) {
                         logger.error("Failed to delete temporary file: " + path, e);
                     }
                 });
        }
    }
} 
