package com.example.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Implementation of AnnotationSynonyms that handles multiple annotation types.
 * Manages synonym mappings for DATE, NER, POS, and DEPENDENCY annotation types.
 * Each type has its own namespace of IDs to avoid collisions.
 */
public class MultiAnnotationSynonyms extends AnnotationSynonyms {
    private static final Logger logger = LoggerFactory.getLogger(MultiAnnotationSynonyms.class);
    
    // File names for each annotation type
    private static final EnumMap<AnnotationType, String> FILE_NAMES = new EnumMap<>(AnnotationType.class);
    static {
        FILE_NAMES.put(AnnotationType.DATE, "date_synonyms.ser");
        FILE_NAMES.put(AnnotationType.NER, "ner_synonyms.ser");
        FILE_NAMES.put(AnnotationType.POS, "pos_synonyms.ser");
        FILE_NAMES.put(AnnotationType.DEPENDENCY, "dependency_synonyms.ser");
    }
    
    // Starting offset for IDs in each namespace to avoid collisions
    private static final EnumMap<AnnotationType, Integer> ID_OFFSETS = new EnumMap<>(AnnotationType.class);
    static {
        ID_OFFSETS.put(AnnotationType.DATE, 1);
        ID_OFFSETS.put(AnnotationType.NER, 10000);
        ID_OFFSETS.put(AnnotationType.POS, 20000);
        ID_OFFSETS.put(AnnotationType.DEPENDENCY, 30000);
    }
    
    // Validation patterns for different annotation types
    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    
    // Bidirectional mappings for each annotation type
    private final EnumMap<AnnotationType, Map<String, Integer>> valueToId = new EnumMap<>(AnnotationType.class);
    private final EnumMap<AnnotationType, Map<Integer, String>> idToValue = new EnumMap<>(AnnotationType.class);
    private final EnumMap<AnnotationType, AtomicInteger> nextIds = new EnumMap<>(AnnotationType.class);
    
    // Storage for serialized mappings
    private final Path baseDir;
    private final EnumMap<AnnotationType, Path> storageFiles = new EnumMap<>(AnnotationType.class);
    
    // State flags
    private final EnumMap<AnnotationType, Boolean> modified = new EnumMap<>(AnnotationType.class);
    private volatile boolean closed = false;

    /**
     * Creates a new MultiAnnotationSynonyms instance, loading existing mappings if available.
     * 
     * @param baseDir Base directory for storing the synonym mappings
     * @throws IOException If there's an error loading existing mappings
     */
    public MultiAnnotationSynonyms(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
        
        // Initialize data structures for each annotation type
        for (AnnotationType type : AnnotationType.values()) {
            valueToId.put(type, new ConcurrentHashMap<>());
            idToValue.put(type, new ConcurrentHashMap<>());
            // Initialize with offset to ensure unique IDs across types
            nextIds.put(type, new AtomicInteger(ID_OFFSETS.get(type)));
            modified.put(type, false);
            storageFiles.put(type, baseDir.resolve(FILE_NAMES.get(type)));
        }
        
        // Load existing mappings
        try {
            loadAllMappings();
            logger.info("Initialized annotation synonyms at {}", baseDir);
        } catch (Exception e) {
            throw new IOException("Failed to initialize annotation synonyms", e);
        }
    }

    @Override
    public int getOrCreateId(String value, AnnotationType type) {
        if (closed) {
            throw new IllegalStateException("MultiAnnotationSynonyms is closed");
        }
        
        validateValue(value, type);
        
        // Check cache first
        Map<String, Integer> typeValueToId = valueToId.get(type);
        Integer existingId = typeValueToId.get(value);
        if (existingId != null) {
            return existingId;
        }

        // Create new ID
        synchronized(this) {
            if (closed) {
                throw new IllegalStateException("MultiAnnotationSynonyms was closed during operation");
            }
            
            // Check again in case another thread created it
            existingId = typeValueToId.get(value);
            if (existingId != null) {
                return existingId;
            }

            int id = nextIds.get(type).getAndIncrement();
            typeValueToId.put(value, id);
            idToValue.get(type).put(id, value);
            modified.put(type, true);
            
            logger.debug("Created new {} synonym: {} -> {}", type, value, id);
            return id;
        }
    }

    @Override
    public String getValue(int id, AnnotationType type) {
        if (closed) {
            throw new IllegalStateException("MultiAnnotationSynonyms is closed");
        }
        
        return idToValue.get(type).get(id);
    }

    @Override
    public int size(AnnotationType type) {
        return valueToId.get(type).size();
    }

    @Override
    public int size() {
        int total = 0;
        for (AnnotationType type : AnnotationType.values()) {
            total += valueToId.get(type).size();
        }
        return total;
    }

    /**
     * Validates that a value is appropriate for the given annotation type.
     * 
     * @param value The value to validate
     * @param type The annotation type
     * @throws IllegalArgumentException if the value is invalid for the given type
     */
    private void validateValue(String value, AnnotationType type) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Value cannot be null or empty");
        }
        
        switch (type) {
            case DATE:
                if (!DATE_PATTERN.matcher(value).matches()) {
                    throw new IllegalArgumentException("Date must be in YYYY-MM-DD format");
                }
                try {
                    LocalDate.parse(value, DATE_FORMATTER);
                } catch (DateTimeParseException e) {
                    throw new IllegalArgumentException("Invalid date: " + value, e);
                }
                break;
            case NER:
                // NER values are typically uppercase like PERSON, LOCATION, etc.
                // No specific validation needed besides non-empty check
                break;
            case POS:
                // POS tags are typically uppercase like NN, VB, JJ, etc.
                // No specific validation needed besides non-empty check
                break;
            case DEPENDENCY:
                // Dependency relations are typically lowercase like nsubj, dobj, etc.
                // No specific validation needed besides non-empty check
                break;
        }
    }

    /**
     * Loads all existing mappings from disk if available.
     */
    private void loadAllMappings() throws IOException {
        for (AnnotationType type : AnnotationType.values()) {
            loadMappings(type);
        }
    }

    /**
     * Loads existing mappings for a specific type from disk if available.
     */
    @SuppressWarnings("unchecked")
    private void loadMappings(AnnotationType type) throws IOException {
        Path storageFile = storageFiles.get(type);
        
        if (Files.exists(storageFile)) {
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(storageFile))) {
                Map<String, Integer> loadedValueToId = (Map<String, Integer>) ois.readObject();
                int maxId = ID_OFFSETS.get(type); // Start with the offset as minimum
                
                for (Map.Entry<String, Integer> entry : loadedValueToId.entrySet()) {
                    valueToId.get(type).put(entry.getKey(), entry.getValue());
                    idToValue.get(type).put(entry.getValue(), entry.getKey());
                    maxId = Math.max(maxId, entry.getValue());
                }
                
                // Ensure next ID is greater than the highest seen ID
                nextIds.get(type).set(maxId + 1);
                logger.info("Loaded {} {} synonyms with next ID {}", 
                           valueToId.get(type).size(), type, nextIds.get(type).get());
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to load " + type + " synonyms", e);
            }
        } else {
            logger.info("No existing {} synonyms found at {}", type, storageFile);
        }
    }

    /**
     * Saves all mappings to disk if they've been modified.
     */
    private void saveAllMappings() throws IOException {
        for (AnnotationType type : AnnotationType.values()) {
            if (modified.get(type)) {
                saveMappings(type);
            }
        }
    }

    /**
     * Saves the current mappings for a specific type to disk.
     */
    private void saveMappings(AnnotationType type) throws IOException {
        Path storageFile = storageFiles.get(type);
        Files.createDirectories(storageFile.getParent());
        
        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(storageFile))) {
            oos.writeObject(new HashMap<>(valueToId.get(type))); // Create a serializable copy
            logger.info("Saved {} {} synonyms", valueToId.get(type).size(), type);
            modified.put(type, false);
        }
    }

    @Override
    public void validateSynonyms() throws IOException {
        for (AnnotationType type : AnnotationType.values()) {
            validateSynonyms(type);
        }
    }

    /**
     * Validates the consistency of synonym mappings for a specific type.
     */
    private void validateSynonyms(AnnotationType type) {
        Map<Integer, String> typeIdToValue = idToValue.get(type);
        Map<String, Integer> typeValueToId = valueToId.get(type);
        
        for (Map.Entry<Integer, String> entry : typeIdToValue.entrySet()) {
            int id = entry.getKey();
            String value = entry.getValue();
            Integer mappedId = typeValueToId.get(value);
            
            if (mappedId == null || mappedId != id) {
                logger.error("Inconsistent {} synonym mappings detected: {} maps to {} but {} maps to {}",
                    type, value, mappedId, id, value);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            try {
                saveAllMappings();
                logger.info("Closed annotation synonyms");
            } catch (Exception e) {
                throw new IOException("Failed to close annotation synonyms", e);
            }
        }
    }
} 