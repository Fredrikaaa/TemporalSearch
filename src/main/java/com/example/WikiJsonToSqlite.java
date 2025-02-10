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
        return extractToSqlite(inputFile, recreate, null);
    }

    public static ExtractionResult extractToSqlite(Path inputFile, boolean recreate, Integer limit) throws Exception {
        // Generate output database name based on input file
        Path outputDb = inputFile.resolveSibling(inputFile.getFileName().toString().replaceFirst("[.][^.]+$", ".db"));
        
        // Count total lines first
        long totalLines = 0;
        try (BufferedReader countReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputFile.toFile()), StandardCharsets.UTF_8))) {
            String line;
            while ((line = countReader.readLine()) != null) {
                try {
                    JsonNode item = objectMapper.readTree(line);
                    // Skip the index information object
                    if (item.has("_type") && item.get("_type").asText().equals("_doc")) {
                        continue;
                    }
                    if (item.has("text")) {
                        totalLines++;
                    }
                } catch (Exception e) {
                    logger.error("Error counting line: {}", e.getMessage());
                }
            }
        }
        logger.info("Found {} lines in input file{}", totalLines,
            limit != null ? String.format(" (will process up to %d entries)", limit) : "");
        
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
                 ProgressBar pb = new ProgressBar("Processing Wikipedia dump", 
                     limit != null ? limit : totalLines)) {

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
                            pb.step();

                            // Check if we've hit the limit
                            if (limit != null && totalEntries >= limit) {
                                // Execute final batch and break
                                pstmt.executeBatch();
                                conn.commit();
                                logger.info("Reached limit of {} entries", limit);
                                break;
                            }
                        }

                        if (++lineCount % 10_000 == 0) {  // Reduced batch size for more frequent updates
                            pstmt.executeBatch();
                            conn.commit();
                            logger.debug("Processed {} lines, {} entries added", lineCount, totalEntries);
                        }

                    } catch (Exception e) {
                        logger.error("Error processing line {}: {}", lineCount + 1, e.getMessage());
                    }
                }

                // Insert any remaining batch
                pstmt.executeBatch();
                conn.commit();
                logger.info("Completed processing {} lines, total {} entries added", lineCount, totalEntries);
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