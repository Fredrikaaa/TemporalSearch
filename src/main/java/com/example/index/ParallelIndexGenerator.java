package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

/**
 * Abstract base class for parallel index generation.
 * Manages thread pool and parallel processing of document batches.
 * @param <T> The type of index entry this generator processes
 */
public abstract class ParallelIndexGenerator<T extends IndexEntry> extends BaseIndexGenerator<T> {
    private static final Logger logger = LoggerFactory.getLogger(ParallelIndexGenerator.class);
    private final ExecutorService executorService;
    private final int threadCount;
    private final Path tempDir;

    protected ParallelIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, String tableName, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, tableName);
        this.threadCount = Math.max(1, threadCount);
        this.executorService = Executors.newFixedThreadPool(this.threadCount);
        this.tempDir = Files.createTempDirectory("parallel-index-");
        
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

    /**
     * Partitions the input entries into batches for parallel processing.
     */
    @Override
    protected List<List<T>> partitionEntries(List<T> entries) {
        if (entries == null || entries.isEmpty()) {
            return new ArrayList<>();
        }

        // Group entries by document ID
        Map<Integer, List<T>> entriesByDoc = new HashMap<>();
        for (T entry : entries) {
            entriesByDoc.computeIfAbsent(entry.getDocumentId(), k -> new ArrayList<>()).add(entry);
        }

        // If we only have one document, return all entries as a single partition
        if (entriesByDoc.size() == 1) {
            List<List<T>> partitions = new ArrayList<>();
            partitions.add(new ArrayList<>(entries));
            return partitions;
        }

        // Sort document IDs to ensure consistent ordering
        List<Integer> docIds = new ArrayList<>(entriesByDoc.keySet());
        Collections.sort(docIds);

        // Calculate optimal number of documents per partition
        int totalDocs = docIds.size();
        int optimalThreadCount = Math.min(threadCount, totalDocs);
        int docsPerPartition = Math.max(1, (totalDocs + optimalThreadCount - 1) / optimalThreadCount);

        List<List<T>> partitions = new ArrayList<>();
        List<T> currentPartition = new ArrayList<>();

        // Create partitions by document
        for (int i = 0; i < docIds.size(); i++) {
            List<T> docEntries = entriesByDoc.get(docIds.get(i));
            
            // If this is the first document in a partition, or if adding this document
            // would exceed the target size, start a new partition
            if (currentPartition.isEmpty() || 
                (i % docsPerPartition == 0 && i > 0)) {
                if (!currentPartition.isEmpty()) {
                    partitions.add(currentPartition);
                }
                currentPartition = new ArrayList<>();
            }
            
            currentPartition.addAll(docEntries);
        }

        // Add the last partition if not empty
        if (!currentPartition.isEmpty()) {
            partitions.add(currentPartition);
        }

        return partitions;
    }

    /**
     * Process a single partition of entries.
     * To be implemented by concrete subclasses.
     */
    @Override
    protected abstract ListMultimap<String, PositionList> processPartition(List<T> partition) throws IOException;

    /**
     * Processes entries in parallel using the thread pool.
     */
    @Override
    protected void processBatch(List<T> entries) throws IOException {
        List<List<T>> partitions = partitionEntries(entries);
        List<Future<ListMultimap<String, PositionList>>> futures = new ArrayList<>();

        // Submit tasks
        for (List<T> partition : partitions) {
            futures.add(executorService.submit(new IndexGenerationTask(partition)));
        }

        // Collect and merge results
        ListMultimap<String, PositionList> result = MultimapBuilder.hashKeys().arrayListValues().build();
        try {
            for (Future<ListMultimap<String, PositionList>> future : futures) {
                ListMultimap<String, PositionList> partialResult = future.get(1, TimeUnit.HOURS);
                mergeResults(result, partialResult);
            }
            
            // Write merged results to LevelDB
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
        } catch (Exception e) {
            throw new IOException("Failed to process entries in parallel", e);
        }
    }

    /**
     * Merges partial results into the final result multimap.
     */
    private void mergeResults(ListMultimap<String, PositionList> target, 
                            ListMultimap<String, PositionList> source) {
        source.asMap().forEach((key, value) -> {
            target.putAll(key, value);
        });
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
     * Cleans up resources used by the generator.
     */
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
        super.close();
    }
} 