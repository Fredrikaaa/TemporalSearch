package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import java.io.*;
import java.util.*;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public abstract class BaseIndexGenerator implements AutoCloseable {
    protected final DB levelDb;
    protected final Set<String> stopwords;
    protected final int batchSize;
    protected final Connection sqliteConn;
    protected final String tableName;

    protected BaseIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, String tableName)
            throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        options.writeBufferSize(64 * 1024 * 1024); // 64MB write buffer
        options.cacheSize(512 * 1024 * 1024L); // 512MB cache

        this.levelDb = factory.open(new File(levelDbPath), options);
        this.stopwords = loadStopwords(stopwordsPath);
        this.batchSize = batchSize;
        this.sqliteConn = sqliteConn;
        this.tableName = tableName;
    }

    private Set<String> loadStopwords(String path) throws IOException {
        Set<String> words = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                words.add(line.trim().toLowerCase());
            }
        }
        return words;
    }

    protected boolean isStopword(String word) {
        return word == null || stopwords.contains(word.toLowerCase());
    }

    protected List<IndexEntry> fetchBatch(int offset) throws SQLException {
        String query = """
                    SELECT a.document_id, a.sentence_id, a.begin_char, a.end_char,
                           a.lemma, a.pos, d.timestamp
                    FROM annotations a
                    JOIN documents d ON a.document_id = d.document_id
                    ORDER BY a.document_id, a.sentence_id, a.begin_char
                    LIMIT ? OFFSET ?
                """;

        List<IndexEntry> entries = new ArrayList<>();
        try (PreparedStatement stmt = sqliteConn.prepareStatement(query)) {
            stmt.setInt(1, batchSize);
            stmt.setInt(2, offset);
            stmt.setFetchSize(batchSize);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    // Parse the ISO-8601 timestamp and convert to LocalDate
                    String timestamp = rs.getString("timestamp");
                    LocalDate date = ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_DATE_TIME)
                            .toLocalDate();

                    entries.add(new IndexEntry(
                            rs.getInt("document_id"),
                            rs.getInt("sentence_id"),
                            rs.getInt("begin_char"),
                            rs.getInt("end_char"),
                            rs.getString("lemma"),
                            rs.getString("pos"),
                            date));
                }
            }
        }
        return entries;
    }

    protected void writeToLevelDb(String key, byte[] value) throws IOException {
        levelDb.put(bytes(key), value);
    }

    protected abstract void processBatch(List<IndexEntry> entries) throws IOException;

    public void generateIndex() throws SQLException, IOException {
        int offset = 0;
        int totalProcessed = 0;
        List<IndexEntry> batch;

        System.out.printf("Generating %s index...%n", tableName);
        long startTime = System.currentTimeMillis();

        while (!(batch = fetchBatch(offset)).isEmpty()) {
            processBatch(batch);
            offset += batchSize;
            totalProcessed += batch.size();

            if (totalProcessed % (batchSize * 10) == 0) {
                System.out.printf("Processed %d entries...%n", totalProcessed);
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("Index generation complete. Processed %d entries in %.2f seconds%n",
                totalProcessed, (endTime - startTime) / 1000.0);
    }

    @Override
    public void close() throws IOException {
        if (levelDb != null) {
            levelDb.close();
        }
    }
}

/**
 * Class representing a single entry from the annotations table
 */
class IndexEntry {
    final int documentId;
    final int sentenceId;
    final int beginChar;
    final int endChar;
    final String lemma;
    final String pos;
    final LocalDate timestamp;

    IndexEntry(int documentId, int sentenceId, int beginChar, int endChar,
            String lemma, String pos, LocalDate timestamp) {
        this.documentId = documentId;
        this.sentenceId = sentenceId;
        this.beginChar = beginChar;
        this.endChar = endChar;
        this.lemma = lemma;
        this.pos = pos;
        this.timestamp = timestamp;
    }
}
