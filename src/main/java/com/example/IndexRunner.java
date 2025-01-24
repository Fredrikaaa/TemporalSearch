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
                .choices("all", "unigram", "bigram", "trigram", "dependency", "ner_date", "pos")
                .setDefault("all")
                .help("Type of index to generate");

        parser.addArgument("--debug")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Enable debug logging to console");

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
                ns.getString("type")
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
            int batchSize, String indexType) throws Exception {
        
        IndexingMetrics metrics = new IndexingMetrics();
        logger.info("Starting index generation with parameters: dbPath={}, indexDir={}, batchSize={}",
            dbPath, indexDir, batchSize);

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             ProgressTracker progress = new ProgressTracker()) {
            
            // Get total document count
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT document_id) FROM annotations");
                if (rs.next()) {
                    int docCount = rs.getInt(1);
                    metrics.incrementDocumentsProcessed(docCount);
                }
            }
            
            // Calculate total steps
            int totalSteps = 0;
            if (indexType.equals("all")) {
                totalSteps = 6; // unigram, bigram, trigram, dependency, ner_date, and pos
            } else {
                totalSteps = 1;
            }
            
            int currentStep = 0;

            if (indexType.equals("all") || indexType.equals("unigram")) {
                currentStep++;
                logger.info("Step {}/{}: Starting unigram index generation", currentStep, totalSteps);
                String unigramDir = indexDir + "/unigram";
                System.out.printf("Stage %d/%d: Unigram%n", currentStep, totalSteps);
                
                try (UnigramIndexGenerator indexer = new UnigramIndexGenerator(
                        unigramDir, stopwordsPath, batchSize, conn, progress)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    // Convert nanoseconds to milliseconds
                    metrics.recordProcessingTime(stepDuration / 1_000_000, "unigram");
                    // Record n-grams generated
                    metrics.addNgramsGenerated(indexer.getTotalNGramsGenerated());
                    metrics.recordStateVerification("unigram_generation", true);
                    // Record index size
                    metrics.recordIndexSize("unigram", calculateDirectorySize(unigramDir));
                }
            }

            if (indexType.equals("all") || indexType.equals("bigram")) {
                currentStep++;
                logger.info("Step {}/{}: Starting bigram index generation", currentStep, totalSteps);
                String bigramDir = indexDir + "/bigram";
                System.out.printf("Stage %d/%d: Bigram%n", currentStep, totalSteps);
                
                try (BigramIndexGenerator indexer = new BigramIndexGenerator(
                        bigramDir, stopwordsPath, batchSize, conn, progress)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    // Convert nanoseconds to milliseconds
                    metrics.recordProcessingTime(stepDuration / 1_000_000, "bigram");
                    // Record n-grams generated
                    metrics.addNgramsGenerated(indexer.getTotalNGramsGenerated());
                    metrics.recordStateVerification("bigram_generation", true);
                    // Record index size
                    metrics.recordIndexSize("bigram", calculateDirectorySize(bigramDir));
                }
            }

            if (indexType.equals("all") || indexType.equals("trigram")) {
                currentStep++;
                logger.info("Step {}/{}: Starting trigram index generation", currentStep, totalSteps);
                String trigramDir = indexDir + "/trigram";
                System.out.printf("Stage %d/%d: Trigram%n", currentStep, totalSteps);
                
                try (TrigramIndexGenerator indexer = new TrigramIndexGenerator(
                        trigramDir, stopwordsPath, batchSize, conn, progress)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    // Convert nanoseconds to milliseconds
                    metrics.recordProcessingTime(stepDuration / 1_000_000, "trigram");
                    // Record n-grams generated
                    metrics.addNgramsGenerated(indexer.getTotalNGramsGenerated());
                    metrics.recordStateVerification("trigram_generation", true);
                    // Record index size
                    metrics.recordIndexSize("trigram", calculateDirectorySize(trigramDir));
                }
            }

            if (indexType.equals("all") || indexType.equals("dependency")) {
                currentStep++;
                logger.info("Step {}/{}: Starting dependency index generation", currentStep, totalSteps);
                String dependencyDir = indexDir + "/dependency";
                System.out.printf("Stage %d/%d: Dependency%n", currentStep, totalSteps);
                
                try (DependencyIndexGenerator indexer = new DependencyIndexGenerator(
                        dependencyDir, stopwordsPath, batchSize, conn, progress)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    // Convert nanoseconds to milliseconds
                    metrics.recordProcessingTime(stepDuration / 1_000_000, "dependency");
                    // Record n-grams generated
                    metrics.addNgramsGenerated(indexer.getTotalNGramsGenerated());
                    metrics.recordStateVerification("dependency_generation", true);
                    // Record index size
                    metrics.recordIndexSize("dependency", calculateDirectorySize(dependencyDir));
                }
            }

            if (indexType.equals("all") || indexType.equals("ner_date")) {
                currentStep++;
                logger.info("Step {}/{}: Starting NER date index generation", currentStep, totalSteps);
                String nerDateDir = indexDir + "/ner_date";
                System.out.printf("Stage %d/%d: NER Date%n", currentStep, totalSteps);
                
                try (NerDateIndexGenerator indexer = new NerDateIndexGenerator(
                        nerDateDir, stopwordsPath, batchSize, conn, progress)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    // Convert nanoseconds to milliseconds
                    metrics.recordProcessingTime(stepDuration / 1_000_000, "ner_date");
                    // Record n-grams generated
                    metrics.addNgramsGenerated(indexer.getTotalNGramsGenerated());
                    metrics.recordStateVerification("ner_date_generation", true);
                    // Record index size
                    metrics.recordIndexSize("ner_date", calculateDirectorySize(nerDateDir));
                }
            }

            if (indexType.equals("all") || indexType.equals("pos")) {
                currentStep++;
                logger.info("Step {}/{}: Starting POS index generation", currentStep, totalSteps);
                String posDir = indexDir + "/pos";
                System.out.printf("Stage %d/%d: POS%n", currentStep, totalSteps);
                
                try (POSIndexGenerator indexer = new POSIndexGenerator(
                        posDir, stopwordsPath, batchSize, conn, progress)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    // Convert nanoseconds to milliseconds
                    metrics.recordProcessingTime(stepDuration / 1_000_000, "pos");
                    // Record n-grams generated
                    metrics.addNgramsGenerated(indexer.getTotalNGramsGenerated());
                    metrics.recordStateVerification("pos_generation", true);
                    // Record index size
                    metrics.recordIndexSize("pos", calculateDirectorySize(posDir));
                }
            }

            logger.info("Index generation completed");
            metrics.logMetrics("indexing_complete");
        }
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
