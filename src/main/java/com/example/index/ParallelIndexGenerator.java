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

/**
 * Abstract base class for parallel index generation.
 * Manages thread pool and parallel processing of document batches.
 */
public abstract class ParallelIndexGenerator extends BaseIndexGenerator {
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
    protected List<List<IndexEntry>> partitionEntries(List<IndexEntry> entries) {
        int batchSize = Math.max(1, entries.size() / threadCount);
        List<List<IndexEntry>> partitions = new ArrayList<>();
        
        for (int i = 0; i < entries.size(); i += batchSize) {
            int end = Math.min(entries.size(), i + batchSize);
            partitions.add(entries.subList(i, end));
        }
        return partitions;
    }

    /**
     * Process a single partition of entries.
     * To be implemented by concrete subclasses.
     */
    protected abstract ListMultimap<String, PositionList> processPartition(List<IndexEntry> partition);

    /**
     * Processes entries in parallel using the thread pool.
     */
    @Override
    protected void processBatch(List<IndexEntry> entries) throws IOException {
        List<List<IndexEntry>> partitions = partitionEntries(entries);
        List<Future<ListMultimap<String, PositionList>>> futures = new ArrayList<>();

        // Submit tasks
        for (List<IndexEntry> partition : partitions) {
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
        private final List<IndexEntry> partition;

        IndexGenerationTask(List<IndexEntry> partition) {
            this.partition = partition;
        }

        @Override
        public ListMultimap<String, PositionList> call() {
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