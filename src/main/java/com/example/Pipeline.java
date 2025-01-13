package com.example;

import com.example.logging.analysis.LogAnalyzer;
import com.example.logging.analysis.LogSummarizer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.nio.file.Path;
import java.util.Map;

public class Pipeline {
    public static void main(String[] args) {
        // Create argument parser
        ArgumentParser parser = ArgumentParsers.newFor("Pipeline").build()
                .defaultHelp(true)
                .description("Process and index text data");

        // Add common arguments
        parser.addArgument("-d", "--db")
                .required(false)
                .help("SQLite database file path");

        parser.addArgument("--debug")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Enable debug logging to console");

        // Add stage selection argument
        parser.addArgument("--stage")
                .choices("all", "annotate", "index", "analyze")
                .setDefault("all")
                .help("Pipeline stage to run ('all' runs annotation and indexing, 'analyze' is a separate post-processing stage)");

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

        // Add analysis-specific arguments
        parser.addArgument("--log-file")
                .help("Path to the log file to analyze");

        parser.addArgument("--report-dir")
                .setDefault("reports")
                .help("Directory for storing analysis reports (default: 'reports')");

        parser.addArgument("--report-format")
                .choices("text", "html", "both")
                .setDefault("both")
                .help("Format of the analysis report (default: both)");

        try {
            // Parse arguments
            Namespace ns = parser.parseArgs(args);
            
            // Set debug mode
            if (ns.getBoolean("debug")) {
                System.setProperty("DEBUG_MODE", "true");
            }

            String stage = ns.getString("stage");
            String dbPath = ns.getString("db");

            // Validate required arguments based on stage
            if ((stage.equals("all") || stage.equals("annotate") || stage.equals("index")) 
                    && dbPath == null) {
                throw new ArgumentParserException("--db is required for annotation and indexing stages", parser);
            }

            if (stage.equals("analyze") && ns.getString("log_file") == null) {
                throw new ArgumentParserException("--log-file is required for analysis stage", parser);
            }

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
                        dbPath,
                        ns.getString("index_dir"),
                        ns.getString("stopwords"),
                        ns.getInt("batch_size"),
                        ns.getString("index_type"));
            }

            if (stage.equals("analyze")) {
                System.out.println("Running log analysis...");
                runAnalysis(
                    ns.getString("log_file"),
                    ns.getString("report_dir"),
                    ns.getString("report_format"));
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

    private static void runAnalysis(String logFile, String reportDir, String format) throws Exception {
        // Create report directory if it doesn't exist
        Path reportPath = Path.of(reportDir);
        if (!reportPath.toFile().exists()) {
            reportPath.toFile().mkdirs();
        }

        // Analyze logs
        LogAnalyzer analyzer = new LogAnalyzer();
        Map<String, Object> results = analyzer.analyzeLogs(Path.of(logFile));

        // Generate reports
        LogSummarizer summarizer = new LogSummarizer();
        if (format.equals("text") || format.equals("both")) {
            Path textReport = reportPath.resolve("analysis_report.txt");
            summarizer.generateReport(results, textReport, "text");
            System.out.println("Generated text report: " + textReport);
        }
        
        if (format.equals("html") || format.equals("both")) {
            Path htmlReport = reportPath.resolve("analysis_report.html");
            summarizer.generateReport(results, htmlReport, "html");
            System.out.println("Generated HTML report: " + htmlReport);
        }

        // Print summary to console
        @SuppressWarnings("unchecked")
        Map<String, Object> summary = (Map<String, Object>) results.get("processing_summary");
        System.out.println("\nAnalysis Summary:");
        System.out.println("----------------");
        System.out.printf("Total Documents Processed: %d%n", summary.get("total_documents_processed"));
        System.out.printf("Total N-grams Generated: %d%n", summary.get("total_ngrams_generated"));
        if (summary.containsKey("avg_ngrams_per_document")) {
            System.out.printf("Average N-grams per Document: %.2f%n", 
                summary.get("avg_ngrams_per_document"));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> errors = (Map<String, Object>) results.get("error_patterns");
        System.out.printf("Total Errors: %d%n", errors.get("total_errors"));
    }
}
