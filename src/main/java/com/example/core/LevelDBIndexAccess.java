package com.example.core;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDate;
import java.util.*;

/**
 * LevelDB-based implementation of IndexAccess interface.
 * Provides efficient key-value storage for index data with support for
 * range queries and atomic updates.
 */
public class LevelDBIndexAccess implements IndexAccess {
    private static final Logger logger = LoggerFactory.getLogger(LevelDBIndexAccess.class);
    private final DB levelDb;
    private final String indexPath;
    private LocalDate minDate;
    private LocalDate maxDate;
    private long entryCount;

    public LevelDBIndexAccess(String indexPath) throws IOException {
        this.indexPath = indexPath;
        Options options = new Options();
        options.createIfMissing(true);
        options.compressionType(CompressionType.SNAPPY);
        options.writeBufferSize(64 * 1024 * 1024); // 64MB write buffer
        options.cacheSize(32 * 1024 * 1024);       // 32MB cache
        
        File indexDir = new File(indexPath);
        if (!indexDir.exists()) {
            indexDir.mkdirs();
        }
        
        this.levelDb = factory.open(indexDir, options);
        initializeMetadata();
    }

    private void initializeMetadata() throws IOException {
        // Initialize temporal range
        byte[] minDateKey = "min_date".getBytes();
        byte[] maxDateKey = "max_date".getBytes();
        byte[] countKey = "entry_count".getBytes();

        byte[] minDateBytes = levelDb.get(minDateKey);
        byte[] maxDateBytes = levelDb.get(maxDateKey);
        byte[] countBytes = levelDb.get(countKey);

        if (minDateBytes != null) {
            minDate = LocalDate.parse(new String(minDateBytes));
        }
        if (maxDateBytes != null) {
            maxDate = LocalDate.parse(new String(maxDateBytes));
        }
        if (countBytes != null) {
            entryCount = Long.parseLong(new String(countBytes));
        }
    }

    private void updateMetadata(Position position) throws IOException {
        LocalDate date = position.getTimestamp();
        
        // Update temporal range
        if (minDate == null || date.isBefore(minDate)) {
            minDate = date;
            levelDb.put("min_date".getBytes(), date.toString().getBytes());
        }
        if (maxDate == null || date.isAfter(maxDate)) {
            maxDate = date;
            levelDb.put("max_date".getBytes(), date.toString().getBytes());
        }
    }

    @Override
    public List<Position> getPositions(String key) throws IOException {
        byte[] value = levelDb.get(key.getBytes());
        if (value == null) {
            return Collections.emptyList();
        }
        return PositionList.deserialize(value).getPositions();
    }

    @Override
    public void addPosition(String key, Position position) throws IOException {
        byte[] keyBytes = key.getBytes();
        byte[] existing = levelDb.get(keyBytes);
        
        PositionList positions;
        if (existing != null) {
            positions = PositionList.deserialize(existing);
            positions.add(position);
        } else {
            positions = new PositionList();
            positions.add(position);
            entryCount++;
            levelDb.put("entry_count".getBytes(), String.valueOf(entryCount).getBytes());
        }
        
        levelDb.put(keyBytes, positions.serialize());
        updateMetadata(position);
    }

    @Override
    public long getEntryCount() throws IOException {
        return entryCount;
    }

    @Override
    public LocalDate[] getTimeRange() throws IOException {
        if (minDate == null || maxDate == null) {
            return null;
        }
        return new LocalDate[] { minDate, maxDate };
    }

    @Override
    public boolean containsKey(String key) throws IOException {
        return levelDb.get(key.getBytes()) != null;
    }

    @Override
    public void close() throws IOException {
        if (levelDb != null) {
            levelDb.close();
        }
    }
} 