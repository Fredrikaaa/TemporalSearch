package com.example;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Pipeline {
    public static void main(String[] args) {
        // Create argument parser
        ArgumentParser parser = ArgumentParsers.newFor("Pipeline").build()
                .defaultHelp(true)
                .description("Process and index text data");

        // Add common arguments
        parser.addArgument("-d", "--db")
                .required(true)
                .help("SQLite database file path");

        // Add stage selection argument
        parser.addArgument("--stage")
                .choices("all", "annotate", "index")
                .setDefault("all")
                .help("Pipeline stage to run (default: all)");

        // Add annotation-specific arguments
        parser.addArgument("-b", "--batch_size")
                .setDefault(1000)
                .type(Integer.class)
                .help("Batch size for processing (default: 1000)");

        parser.addArgument("-l", "--limit")
                .type(Integer.class)
                .help("Limit the number of documents to process");

        parser.addArgument("-t", "--threads")
                .setDefault(8)
                .type(Integer.class)
                .help("Number of threads for CoreNLP (default: 8)");

        // Add index-specific arguments
        parser.addArgument("--index-dir")
                .setDefault("index")
                .help("Directory for storing indexes (default: 'index')");

        parser.addArgument("--stopwords")
                .setDefault("stopwords.txt")
                .help("Path to stopwords file (default: stopwords.txt)");

        parser.addArgument("--index-type")
                .choices("unigram", "bigram", "trigram", "all")
                .setDefault("all")
                .help("Type of index to generate (default: all)");

        try {
            // Parse arguments
            Namespace ns = parser.parseArgs(args);
            String stage = ns.getString("stage");
            String dbPath = ns.getString("db");

            // Run selected pipeline stages
            if (stage.equals("all") || stage.equals("annotate")) {
                System.out.println("Running annotation stage...");
                Annotations.main(new String[] {
                        "-d", dbPath,
                        "-b", ns.getInt("batch_size").toString(),
                        "-t", ns.getInt("threads").toString(),
                        "-l", ns.getInt("limit").toString()
                });
            }

            if (stage.equals("all") || stage.equals("index")) {
                System.out.println("Running indexing stage...");
                IndexRunner.runIndexing(
                        ns.getString("db"),
                        ns.getString("index_dir"),
                        ns.getString("stopwords"),
                        ns.getInt("batch_size"),
                        ns.getString("index_type"));
            }

            System.out.println("Pipeline completed successfully!");

        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error running pipeline: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
