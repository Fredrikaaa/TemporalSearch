package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import me.tongfei.progressbar.ProgressBar;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.*;

public class WikiJsonToSqlite {
    private static final Logger logger = LoggerFactory.getLogger(WikiJsonToSqlite.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static class ExtractionResult {
        public final Path outputDb;
        public final long totalEntries;

        public ExtractionResult(Path outputDb, long totalEntries) {
            this.outputDb = outputDb;
            this.totalEntries = totalEntries;
        }
    }

    public static ExtractionResult extractToSqlite(Path inputFile, boolean recreate) throws Exception {
        // Generate output database name based on input file
        Path outputDb = inputFile.resolveSibling(inputFile.getFileName().toString().replaceFirst("[.][^.]+$", ".db"));
        
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + outputDb.toString())) {
            // Enable WAL mode and other optimizations for better performance
            try (Statement pragma = conn.createStatement()) {
                pragma.execute("PRAGMA journal_mode=WAL");
                pragma.execute("PRAGMA synchronous=NORMAL");
                pragma.execute("PRAGMA temp_store=MEMORY");
                pragma.execute("PRAGMA cache_size=-2000"); // Use 2GB cache
            }

            try (Statement stmt = conn.createStatement()) {
                if (recreate) {
                    stmt.execute("DROP TABLE IF EXISTS documents");
                }
                stmt.execute("""
                    CREATE TABLE IF NOT EXISTS documents (
                        document_id INTEGER PRIMARY KEY,
                        title TEXT,
                        text TEXT,
                        timestamp TEXT
                    )
                """);
            }

            conn.setAutoCommit(false);
            long totalEntries = 0;
            long lineCount = 0;

            String insertSql = "INSERT INTO documents (title, text, timestamp) VALUES (?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql);
                 BufferedReader reader = new BufferedReader(
                     new InputStreamReader(new FileInputStream(inputFile.toFile()), StandardCharsets.UTF_8));
                 ProgressBar pb = new ProgressBar("Processing Wikipedia dump", 100_000_000)) { // Estimate 100M lines

                String line;
                while ((line = reader.readLine()) != null) {
                    try {
                        JsonNode item = objectMapper.readTree(line);

                        // Skip the index information object
                        if (item.has("_type") && item.get("_type").asText().equals("_doc")) {
                            continue;
                        }

                        if (item.has("text")) {
                            pstmt.setString(1, getTextValue(item, "title"));
                            pstmt.setString(2, getTextValue(item, "text"));
                            pstmt.setString(3, getTextValue(item, "timestamp"));
                            pstmt.addBatch();
                            totalEntries++;
                        }

                        if (++lineCount % 100_000 == 0) {
                            pstmt.executeBatch();
                            conn.commit();
                            logger.info("Processed {} lines, {} entries added", lineCount, totalEntries);
                        }
                        pb.step();

                    } catch (Exception e) {
                        logger.error("Error processing line {}: {}", lineCount + 1, e.getMessage());
                    }
                }

                // Insert any remaining batch
                pstmt.executeBatch();
                conn.commit();
            }

            return new ExtractionResult(outputDb, totalEntries);
        }
    }

    private static String getTextValue(JsonNode node, String field) {
        return node.has(field) ? node.get(field).asText() : null;
    }

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("WikiJsonToSqlite").build()
                .defaultHelp(true)
                .description("Extract text from Wikipedia JSON dump to SQLite");

        parser.addArgument("-f", "--file")
                .required(true)
                .help("Input JSON file path");

        parser.addArgument("--recreate")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Drop and recreate the documents table if it exists");

        try {
            Namespace ns = parser.parseArgs(args);
            Path inputFile = Path.of(ns.getString("file"));
            boolean recreate = ns.getBoolean("recreate");

            ExtractionResult result = extractToSqlite(inputFile, recreate);
            System.out.printf("Extraction complete. %d entries added to database: %s%n",
                    result.totalEntries, result.outputDb);

        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Error during extraction: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
} 