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

        parser.addArgument("--index-type")
                .choices("unigram", "bigram", "trigram", "dependency", "all")
                .setDefault("all")
                .help("Type of index to generate (default: all)");

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

    private static long countEntries(Connection conn, String indexType) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            String table = switch (indexType) {
                case "dependency" -> "dependencies";
                default -> "annotations";
            };
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM " + table);
            if (rs.next()) {
                return rs.getLong("total");
            }
            return 0;
        }
    }

    public static void runIndexing(String dbPath, String indexDir, String stopwordsPath,
            int batchSize, String indexType) throws Exception {
        
        IndexingMetrics metrics = new IndexingMetrics();
        logger.info("Starting index generation with parameters: dbPath={}, indexDir={}, batchSize={}",
            dbPath, indexDir, batchSize);

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             ProgressTracker progress = new ProgressTracker()) {
            
            int totalSteps = indexType.equals("all") ? 4 : 1;
            long totalWork = indexType.equals("all") ? 4 : 1; // Just track number of indexes
            progress.startOverall("Generating indexes", totalWork);
            int currentStep = 0;

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
                }
                progress.updateOverall(1);
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
                }
                progress.updateOverall(1);
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
                }
                progress.updateOverall(1);
            }

            if (indexType.equals("all") || indexType.equals("dependency")) {
                currentStep++;
                logger.info("Step {}/{}: Starting dependency index generation", currentStep, totalSteps);
                String dependencyDir = indexDir + "/dependency";
                
                try (DependencyIndexGenerator indexer = new DependencyIndexGenerator(
                        dependencyDir, stopwordsPath, batchSize, conn)) {
                    long stepStart = System.nanoTime();
                    indexer.generateIndex();
                    long stepDuration = System.nanoTime() - stepStart;
                    
                    metrics.recordProcessingTime(stepDuration);
                }
                progress.updateOverall(1);
            }

            progress.completeOverall();
            logger.info("Index generation completed");
            metrics.logMetrics(logger, "indexing_complete");
        }
    }
}
