package com.example;

import com.example.index.*;
import com.example.logging.IndexingMetrics;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

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
                .setDefault("index")
                .help("Directory for storing indexes (default: 'index')");

        parser.addArgument("--stopwords")
                .setDefault("stopwords.txt")
                .help("Path to stopwords file (default: stopwords.txt)");

        parser.addArgument("--batch-size")
                .setDefault(1000)
                .type(Integer.class)
                .help("Batch size for processing (default: 1000)");

        parser.addArgument("--index-type")
                .choices("unigram", "bigram", "trigram", "all")
                .setDefault("all")
                .help("Type of index to generate (default: all)");

        try {
            // Parse arguments
            Namespace ns = parser.parseArgs(args);
            runIndexing(
                ns.getString("db"),
                ns.getString("index_dir"),
                ns.getString("stopwords"),
                ns.getInt("batch_size"),
                ns.getString("index_type")
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

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            int totalSteps = indexType.equals("all") ? 3 : 1;
            int currentStep = 0;

            // Track overall processing time
            long startTime = System.nanoTime();

            if (indexType.equals("all") || indexType.equals("unigram")) {
                currentStep++;
                logger.info("Step {}/{}: Starting unigram index generation", currentStep, totalSteps);
                String unigramDir = indexDir + "/unigram";
                
                try (UnigramIndexGenerator indexer = new UnigramIndexGenerator(
                        unigramDir, stopwordsPath, batchSize, conn)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    metrics.recordProcessingTime(stepDuration);
                    metrics.logMetrics(logger, "unigram_complete");
                }
                
                metrics.recordStateVerification("unigram_generation", true);
            }

            if (indexType.equals("all") || indexType.equals("bigram")) {
                currentStep++;
                logger.info("Step {}/{}: Starting bigram index generation", currentStep, totalSteps);
                String bigramDir = indexDir + "/bigram";
                
                try (BigramIndexGenerator indexer = new BigramIndexGenerator(
                        bigramDir, stopwordsPath, batchSize, conn)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    metrics.recordProcessingTime(stepDuration);
                    metrics.logMetrics(logger, "bigram_complete");
                }
                
                metrics.recordStateVerification("bigram_generation", true);
            }

            if (indexType.equals("all") || indexType.equals("trigram")) {
                currentStep++;
                logger.info("Step {}/{}: Starting trigram index generation", currentStep, totalSteps);
                String trigramDir = indexDir + "/trigram";
                
                try (TrigramIndexGenerator indexer = new TrigramIndexGenerator(
                        trigramDir, stopwordsPath, batchSize, conn)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    metrics.recordProcessingTime(stepDuration);
                    metrics.logMetrics(logger, "trigram_complete");
                }
                
                metrics.recordStateVerification("trigram_generation", true);
            }

            // Log final metrics
            long totalDuration = System.nanoTime() - startTime;
            metrics.recordProcessingTime(totalDuration);
            metrics.logMetrics(logger, "indexing_complete");

            logger.info("Index generation completed successfully in {} ms", totalDuration / 1_000_000.0);
        } catch (Exception e) {
            metrics.recordStateVerification("index_generation", false);
            metrics.logMetrics(logger, "indexing_failed");
            throw e;
        }
    }
}
