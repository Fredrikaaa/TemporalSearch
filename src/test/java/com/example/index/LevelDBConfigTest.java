package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class LevelDBConfigTest {
    private Path tempDir;
    private DB db;
    
    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("leveldb-test");
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.close();
        }
        Files.walk(tempDir)
             .sorted(Comparator.reverseOrder())
             .forEach(path -> {
                 try {
                     Files.deleteIfExists(path);
                 } catch (IOException e) {
                     // Ignore cleanup errors
                 }
             });
    }
    
    @Test
    void testOptimizedConfiguration() throws IOException {
        // Create DB with optimized config
        Options options = LevelDBConfig.createOptimizedOptions();
        db = factory.open(tempDir.resolve("optimized").toFile(), options);
        
        // Write test data in batches
        WriteBatch batch = db.createWriteBatch();
        try {
            // Write 100K entries in batches of 10K
            for (int i = 0; i < 100_000; i++) {
                String key = String.format("key-%06d", i);
                String value = "value-" + i;
                batch.put(bytes(key), bytes(value));
                
                if ((i + 1) % LevelDBConfig.BATCH_SIZE == 0) {
                    db.write(batch);
                    batch.close();
                    batch = db.createWriteBatch();
                }
            }
        } finally {
            batch.close();
        }
        
        // Verify random reads
        Random random = new Random(42);
        for (int i = 0; i < 1000; i++) {
            int keyNum = random.nextInt(100_000);
            String key = String.format("key-%06d", keyNum);
            byte[] value = db.get(bytes(key));
            assertNotNull(value, "Value should exist for key: " + key);
            assertEquals("value-" + keyNum, new String(value));
        }
    }
    
    @Test
    void testKeyPrefixes() throws IOException {
        Options options = LevelDBConfig.createOptimizedOptions();
        db = factory.open(tempDir.resolve("prefixes").toFile(), options);
        
        // Test metadata key
        String metaKey = KeyPrefixes.createMetaKey("test-term");
        db.put(bytes(metaKey), bytes("meta-value"));
        
        // Test positions key
        String posKey = KeyPrefixes.createPositionsKey("test-term");
        db.put(bytes(posKey), bytes("positions-value"));
        
        // Verify key separation
        byte[] metaValue = db.get(bytes(metaKey));
        byte[] posValue = db.get(bytes(posKey));
        
        assertNotNull(metaValue, "Metadata value should exist");
        assertNotNull(posValue, "Positions value should exist");
        assertEquals("meta-value", new String(metaValue));
        assertEquals("positions-value", new String(posValue));
        
        // Verify term extraction
        assertEquals("test-term", KeyPrefixes.extractTerm(metaKey));
        assertEquals("test-term", KeyPrefixes.extractTerm(posKey));
    }
} 