package com.example;

import com.example.index.*;
import com.example.logging.IndexingMetrics;
import com.example.logging.ProgressTracker;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class IndexRunner {
    private static final Logger logger = LoggerFactory.getLogger(IndexRunner.class);

    public static void main(String[] args) {
        // Create focused argument parser for indexing
        ArgumentParser parser = ArgumentParsers.newFor("IndexRunner").build()
                .defaultHelp(true)
                .description("Create indexes from annotated database");

        parser.addArgument("-d", "--db")
                .required(true)
                .help("SQLite database file path");

        parser.addArgument("--index-dir")
                .setDefault("indexes")
                .help("Directory for storing indexes (default: 'indexes')");

        parser.addArgument("--stopwords")
                .setDefault("stopwords.txt")
                .help("Path to stopwords file (default: stopwords.txt)");

        parser.addArgument("--batch-size")
                .setDefault(1000)
                .type(Integer.class)
                .help("Batch size for processing (default: 1000)");

        parser.addArgument("-t", "--type")
                .choices("all", "unigram", "bigram", "trigram", "dependency", "ner_date", "pos", "hypernym")
                .setDefault("all")
                .help("Type of index to generate");

        parser.addArgument("--debug")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Enable debug logging to console");

        parser.addArgument("-k", "--preserve-index")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Keep existing index data if present");

        parser.addArgument("-l", "--limit")
                .type(Integer.class)
                .help("Maximum number of documents to process");

        try {
            // Parse arguments
            Namespace ns = parser.parseArgs(args);
            
            // Set debug mode
            if (ns.getBoolean("debug")) {
                System.setProperty("DEBUG_MODE", "true");
            }

            runIndexing(
                ns.getString("db"),
                ns.getString("index_dir"),
                ns.getString("stopwords"),
                ns.getInt("batch_size"),
                ns.getString("type"),
                ns.getBoolean("preserve_index"),
                ns.getInt("limit")
            );
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Error generating index: {}", e.getMessage(), e);
            System.exit(1);
        }
    }


    public static void runIndexing(String dbPath, String indexDir, String stopwordsPath,
            int batchSize, String indexType, boolean preserveIndex) throws Exception {
        runIndexing(dbPath, indexDir, stopwordsPath, batchSize, indexType, preserveIndex, null);
    }

    public static void runIndexing(String dbPath, String indexDir, String stopwordsPath,
            int batchSize, String indexType, boolean preserveIndex, Integer limit) throws Exception {
        
        IndexingMetrics metrics = new IndexingMetrics();
        ProgressTracker progress = new ProgressTracker();
        
        logger.info("Starting index generation with parameters: dbPath={}, indexDir={}, batchSize={}, preserveIndex={}, limit={}",
            dbPath, indexDir, batchSize, preserveIndex, limit);

        // Create index configuration
        IndexConfig indexConfig = new IndexConfig.Builder()
            .withPreserveExistingIndex(preserveIndex)
            .withLimit(limit)
            .build();

        // Ensure index directory exists
        Path indexPath = Paths.get(indexDir);
        if (!preserveIndex && Files.exists(indexPath)) {
            logger.info("Cleaning existing index directory: {}", indexPath);
            Files.walk(indexPath)
                 .sorted((a, b) -> b.compareTo(a))
                 .forEach(path -> {
                     try {
                         Files.deleteIfExists(path);
                     } catch (IOException e) {
                         logger.warn("Could not delete: {} ({})", path, e.getMessage());
                     }
                 });
        }
        Files.createDirectories(indexPath);

        // Connect to database
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            // Get total document count for progress tracking
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT document_id) FROM annotations");
                if (rs.next()) {
                    long totalDocs = rs.getLong(1);
                    progress.startIndex("documents", totalDocs);
                    logger.info("Found {} documents to process", totalDocs);
                }
            }

            // Create and run generators based on type
            try {
                if (indexType.equals("all") || indexType.equals("unigram")) {
                    metrics.startBatch(batchSize);
                    try (UnigramIndexGenerator gen = new UnigramIndexGenerator(
                            indexDir + "/unigram", stopwordsPath, conn, progress, indexConfig)) {
                        gen.generateIndex();
                        metrics.recordBatchSuccess();
                    } catch (Exception e) {
                        metrics.recordBatchFailure();
                        logger.error("Error generating unigram index: {}", e.getMessage(), e);
                    }
                }

                if (indexType.equals("all") || indexType.equals("bigram")) {
                    metrics.startBatch(batchSize);
                    try (BigramIndexGenerator gen = new BigramIndexGenerator(
                            indexDir + "/bigram", stopwordsPath, conn, progress, indexConfig)) {
                        gen.generateIndex();
                        metrics.recordBatchSuccess();
                    } catch (Exception e) {
                        metrics.recordBatchFailure();
                        logger.error("Error generating bigram index: {}", e.getMessage(), e);
                    }
                }

                if (indexType.equals("all") || indexType.equals("trigram")) {
                    metrics.startBatch(batchSize);
                    try (TrigramIndexGenerator gen = new TrigramIndexGenerator(
                            indexDir + "/trigram", stopwordsPath, conn, progress, indexConfig)) {
                        gen.generateIndex();
                        metrics.recordBatchSuccess();
                    } catch (Exception e) {
                        metrics.recordBatchFailure();
                        logger.error("Error generating trigram index: {}", e.getMessage(), e);
                    }
                }

                if (indexType.equals("all") || indexType.equals("dependency")) {
                    metrics.startBatch(batchSize);
                    try (DependencyIndexGenerator gen = new DependencyIndexGenerator(
                            indexDir + "/dependency", stopwordsPath, conn, progress, indexConfig)) {
                        gen.generateIndex();
                        metrics.recordBatchSuccess();
                    } catch (Exception e) {
                        metrics.recordBatchFailure();
                        logger.error("Error generating dependency index: {}", e.getMessage(), e);
                    }
                }

                if (indexType.equals("all") || indexType.equals("ner_date")) {
                    metrics.startBatch(batchSize);
                    try (NerDateIndexGenerator gen = new NerDateIndexGenerator(
                            indexDir + "/ner_date", stopwordsPath, conn, progress, indexConfig)) {
                        gen.generateIndex();
                        metrics.recordBatchSuccess();
                    } catch (Exception e) {
                        metrics.recordBatchFailure();
                        logger.error("Error generating NER date index: {}", e.getMessage(), e);
                    }
                }

                if (indexType.equals("all") || indexType.equals("pos")) {
                    metrics.startBatch(batchSize);
                    try (POSIndexGenerator gen = new POSIndexGenerator(
                            indexDir + "/pos", stopwordsPath, conn, progress, indexConfig)) {
                        gen.generateIndex();
                        metrics.recordBatchSuccess();
                    } catch (Exception e) {
                        metrics.recordBatchFailure();
                        logger.error("Error generating POS index: {}", e.getMessage(), e);
                    }
                }

                if (indexType.equals("all") || indexType.equals("hypernym")) {
                    metrics.startBatch(batchSize);
                    try (HypernymIndexGenerator gen = new HypernymIndexGenerator(
                            indexDir + "/hypernym", stopwordsPath, conn, progress, indexConfig)) {
                        gen.generateIndex();
                        metrics.recordBatchSuccess();
                    } catch (Exception e) {
                        metrics.recordBatchFailure();
                        logger.error("Error generating hypernym index: {}", e.getMessage(), e);
                    }
                }

            } finally {
                // Log final metrics
                metrics.logIndexingMetrics();
                progress.completeIndex();
            }
        }

        logger.info("Index generation completed successfully");
    }

    private static long calculateDirectorySize(String path) {
        try {
            Path dir = Paths.get(path);
            if (!Files.exists(dir)) {
                return 0L;
            }
            return Files.walk(dir)
                .filter(p -> !Files.isDirectory(p))
                .mapToLong(p -> {
                    try {
                        return Files.size(p);
                    } catch (IOException e) {
                        logger.warn("Failed to get size for file: {}", p);
                        return 0L;
                    }
                })
                .sum();
        } catch (IOException e) {
            logger.warn("Failed to calculate directory size for: {}", path);
            return 0L;
        }
    }
}
