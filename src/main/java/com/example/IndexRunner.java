package com.example;

import com.example.index.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.sql.Connection;
import java.sql.DriverManager;

public class IndexRunner {
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

            // Run indexing process based on selected type
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + ns.getString("db"))) {
                String indexType = ns.getString("index_type");
                String baseDir = ns.getString("index_dir");

                if (indexType.equals("all") || indexType.equals("unigram")) {
                    System.out.println("Generating unigram index...");
                    String unigramDir = baseDir + "/unigram";
                    try (UnigramIndexGenerator indexer = new UnigramIndexGenerator(
                            unigramDir,
                            ns.getString("stopwords"),
                            ns.getInt("batch_size"),
                            conn)) {
                        indexer.generateIndex();
                    }
                }

                if (indexType.equals("all") || indexType.equals("bigram")) {
                    System.out.println("Generating bigram index...");
                    String bigramDir = baseDir + "/bigram";
                    try (BigramIndexGenerator indexer = new BigramIndexGenerator(
                            bigramDir,
                            ns.getString("stopwords"),
                            ns.getInt("batch_size"),
                            conn)) {
                        indexer.generateIndex();
                    }
                }

                if (indexType.equals("all") || indexType.equals("trigram")) {
                    System.out.println("Generating trigram index...");
                    String trigramDir = baseDir + "/trigram";
                    try (TrigramIndexGenerator indexer = new TrigramIndexGenerator(
                            trigramDir,
                            ns.getString("stopwords"),
                            ns.getInt("batch_size"),
                            conn)) {
                        indexer.generateIndex();
                    }
                }

                System.out.println("Index generation complete!");
            }

        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error generating index: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Method that can be called from Pipeline or other classes
    public static void runIndexing(String dbPath, String indexDir, String stopwordsPath,
            int batchSize, String indexType) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            // Create subdirectories for each index type
            if (indexType.equals("all") || indexType.equals("unigram")) {
                String unigramDir = indexDir + "/unigram";
                try (UnigramIndexGenerator indexer = new UnigramIndexGenerator(
                        unigramDir, stopwordsPath, batchSize, conn)) {
                    indexer.generateIndex();
                }
            }

            if (indexType.equals("all") || indexType.equals("bigram")) {
                String bigramDir = indexDir + "/bigram";
                try (BigramIndexGenerator indexer = new BigramIndexGenerator(
                        bigramDir, stopwordsPath, batchSize, conn)) {
                    indexer.generateIndex();
                }
            }

            if (indexType.equals("all") || indexType.equals("trigram")) {
                String trigramDir = indexDir + "/trigram";
                try (TrigramIndexGenerator indexer = new TrigramIndexGenerator(
                        trigramDir, stopwordsPath, batchSize, conn)) {
                    indexer.generateIndex();
                }
            }
        }
    }
}
