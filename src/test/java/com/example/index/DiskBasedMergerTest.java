package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;

class DiskBasedMergerTest {
    private Path tempDir;
    private List<Path> tempIndexPaths;
    private Path outputPath;
    private LocalDate testDate;
    private List<DB> openDatabases;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create temporary directories
        tempDir = Files.createTempDirectory("merger-test-");
        outputPath = tempDir.resolve("merged-index");
        tempIndexPaths = new ArrayList<>();
        testDate = LocalDate.of(2024, 1, 20);
        openDatabases = new ArrayList<>();
        
        // Create test data in temporary indexes
        createTestIndexes();
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // Close all open databases
        for (DB db : openDatabases) {
            try {
                db.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        openDatabases.clear();
        
        // Clean up temporary files
        Files.walk(tempDir)
             .sorted(Comparator.reverseOrder())
             .forEach(path -> {
                 try {
                     Files.delete(path);
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
             });
    }
    
    private void createTestIndexes() throws IOException {
        // Create three test indexes with overlapping entries
        Map<String, List<Position>> index1 = new HashMap<>();
        index1.put("quick", Arrays.asList(new Position(1, 1, 0, 5, testDate)));
        index1.put("brown", Arrays.asList(new Position(1, 1, 6, 11, testDate)));
        
        Map<String, List<Position>> index2 = new HashMap<>();
        index2.put("brown", Arrays.asList(new Position(2, 1, 0, 5, testDate)));
        index2.put("fox", Arrays.asList(new Position(2, 1, 6, 9, testDate)));
        
        Map<String, List<Position>> index3 = new HashMap<>();
        index3.put("fox", Arrays.asList(new Position(3, 1, 0, 3, testDate)));
        index3.put("jumps", Arrays.asList(new Position(3, 1, 4, 9, testDate)));
        
        // Write test data to temporary LevelDB files
        tempIndexPaths.add(createTempIndex(index1, "index1"));
        tempIndexPaths.add(createTempIndex(index2, "index2"));
        tempIndexPaths.add(createTempIndex(index3, "index3"));
    }
    
    private Path createTempIndex(Map<String, List<Position>> data, String name) throws IOException {
        Path indexPath = tempDir.resolve(name);
        Options options = new Options();
        options.createIfMissing(true);
        
        DB db = factory.open(indexPath.toFile(), options);
        openDatabases.add(db);
        
        for (Map.Entry<String, List<Position>> entry : data.entrySet()) {
            PositionList positions = new PositionList();
            for (Position pos : entry.getValue()) {
                positions.add(pos);
            }
            db.put(bytes(entry.getKey()), positions.serialize());
        }
        
        db.close();
        openDatabases.remove(db);
        
        return indexPath;
    }
    
    @Test
    void testMergeIndexes() throws IOException {
        try (DiskBasedMerger merger = new DiskBasedMerger(tempDir)) {
            merger.mergeIndexes(tempIndexPaths, outputPath);
            
            // Verify merged results
            DB db = factory.open(outputPath.toFile(), new Options());
            openDatabases.add(db);
            
            try {
                // Check "brown" has positions from both index1 and index2
                byte[] brownValue = db.get(bytes("brown"));
                assertNotNull(brownValue, "brown should exist in merged index");
                PositionList brownPositions = PositionList.deserialize(brownValue);
                assertEquals(2, brownPositions.size(), "brown should have 2 positions");
                
                // Check "fox" has positions from both index2 and index3
                byte[] foxValue = db.get(bytes("fox"));
                assertNotNull(foxValue, "fox should exist in merged index");
                PositionList foxPositions = PositionList.deserialize(foxValue);
                assertEquals(2, foxPositions.size(), "fox should have 2 positions");
                
                // Check single-occurrence words
                assertNotNull(db.get(bytes("quick")), "quick should exist in merged index");
                assertNotNull(db.get(bytes("jumps")), "jumps should exist in merged index");
            } finally {
                db.close();
                openDatabases.remove(db);
            }
        }
    }
    
    @Test
    void testMergeWithEmptyIndexes() throws IOException {
        // Create an empty index
        Path emptyIndex = createTempIndex(new HashMap<>(), "empty");
        tempIndexPaths.add(emptyIndex);
        
        try (DiskBasedMerger merger = new DiskBasedMerger(tempDir)) {
            merger.mergeIndexes(tempIndexPaths, outputPath);
            
            // Verify merged results are same as without empty index
            DB db = factory.open(outputPath.toFile(), new Options());
            openDatabases.add(db);
            
            try {
                // All words should still be present
                assertNotNull(db.get(bytes("quick")), "quick should exist in merged index");
                assertNotNull(db.get(bytes("brown")), "brown should exist in merged index");
                assertNotNull(db.get(bytes("fox")), "fox should exist in merged index");
                assertNotNull(db.get(bytes("jumps")), "jumps should exist in merged index");
            } finally {
                db.close();
                openDatabases.remove(db);
            }
        }
    }
    
    @Test
    void testMergeWithLargeNumberOfIndexes() throws IOException {
        // Create 20 small indexes to test multi-pass merging
        for (int i = 0; i < 20; i++) {
            Map<String, List<Position>> data = new HashMap<>();
            data.put("word" + i, Arrays.asList(new Position(i, 1, 0, 5, testDate)));
            tempIndexPaths.add(createTempIndex(data, "index" + i));
        }
        
        try (DiskBasedMerger merger = new DiskBasedMerger(tempDir, 64, 5)) { // Use smaller merge factor
            merger.mergeIndexes(tempIndexPaths, outputPath);
            
            // Verify all words are present in merged index
            DB db = factory.open(outputPath.toFile(), new Options());
            openDatabases.add(db);
            
            try {
                for (int i = 0; i < 20; i++) {
                    assertNotNull(db.get(bytes("word" + i)), 
                        "word" + i + " should exist in merged index");
                }
            } finally {
                db.close();
                openDatabases.remove(db);
            }
        }
    }
    
    private static byte[] bytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
} 