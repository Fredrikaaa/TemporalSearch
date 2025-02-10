package com.example.core;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

/**
 * LevelDB-based implementation of IndexAccess.
 * Provides efficient key-value storage for index data.
 */
public class LevelDbIndexAccess implements IndexAccess {
    private static final Logger logger = LoggerFactory.getLogger(LevelDbIndexAccess.class);
    private static final String DELIMITER = "\0";
    private static final String META_PREFIX = "_meta_";
    private static final String COUNT_KEY = META_PREFIX + "count";
    private static final String MIN_DATE_KEY = META_PREFIX + "min_date";
    private static final String MAX_DATE_KEY = META_PREFIX + "max_date";

    private final DB levelDb;
    private long entryCount;
    private LocalDate minDate;
    private LocalDate maxDate;

    public LevelDbIndexAccess(String dbPath) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        this.levelDb = factory.open(new File(dbPath), options);
        loadMetadata();
    }

    private void loadMetadata() {
        byte[] countBytes = levelDb.get(bytes(COUNT_KEY));
        if (countBytes != null) {
            this.entryCount = Long.parseLong(new String(countBytes, StandardCharsets.UTF_8));
        }

        byte[] minDateBytes = levelDb.get(bytes(MIN_DATE_KEY));
        if (minDateBytes != null) {
            this.minDate = LocalDate.parse(new String(minDateBytes, StandardCharsets.UTF_8));
        }

        byte[] maxDateBytes = levelDb.get(bytes(MAX_DATE_KEY));
        if (maxDateBytes != null) {
            this.maxDate = LocalDate.parse(new String(maxDateBytes, StandardCharsets.UTF_8));
        }
    }

    private void updateMetadata() {
        levelDb.put(bytes(COUNT_KEY), bytes(String.valueOf(entryCount)));
        if (minDate != null) {
            levelDb.put(bytes(MIN_DATE_KEY), bytes(minDate.toString()));
        }
        if (maxDate != null) {
            levelDb.put(bytes(MAX_DATE_KEY), bytes(maxDate.toString()));
        }
    }

    @Override
    public List<Position> getPositions(String key) throws IOException {
        List<Position> positions = new ArrayList<>();
        byte[] value = levelDb.get(bytes(key));
        if (value != null) {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(value))) {
                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    positions.add(readPosition(in));
                }
            }
        }
        return positions;
    }

    @Override
    public void addPosition(String key, Position position) throws IOException {
        List<Position> positions = getPositions(key);
        positions.add(position);
        
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(positions.size());
            for (Position pos : positions) {
                writePosition(out, pos);
            }
            levelDb.put(bytes(key), bos.toByteArray());
        }
        
        entryCount++;
        updateMetadata();
        
        // Update date range
        LocalDate date = position.getTimestamp();
        if (minDate == null || date.isBefore(minDate)) {
            minDate = date;
        }
        if (maxDate == null || date.isAfter(maxDate)) {
            maxDate = date;
        }
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    @Override
    public LocalDate[] getTimeRange() {
        return new LocalDate[]{minDate, maxDate};
    }

    @Override
    public boolean containsKey(String key) {
        return levelDb.get(bytes(key)) != null;
    }

    @Override
    public void close() throws IOException {
        if (levelDb != null) {
            updateMetadata();
            levelDb.close();
        }
    }

    private static byte[] bytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private static Position readPosition(DataInputStream in) throws IOException {
        int documentId = in.readInt();
        int sentenceId = in.readInt();
        int beginChar = in.readInt();
        int endChar = in.readInt();
        LocalDate timestamp = LocalDate.parse(in.readUTF());
        return new Position(documentId, sentenceId, beginChar, endChar, timestamp);
    }

    private static void writePosition(DataOutputStream out, Position position) throws IOException {
        out.writeInt(position.getDocumentId());
        out.writeInt(position.getSentenceId());
        out.writeInt(position.getBeginChar());
        out.writeInt(position.getEndChar());
        out.writeUTF(position.getTimestamp().toString());
    }
} 