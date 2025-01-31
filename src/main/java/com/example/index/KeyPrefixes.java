package com.example.index;

/**
 * Defines key prefixes for separating different types of data in LevelDB.
 * This separation helps reduce write amplification and improve read performance.
 */
public class KeyPrefixes {
    public static final String META = "meta:";
    public static final String POSITIONS = "pos:";
    
    /**
     * Creates a key for storing term metadata.
     * @param term The term to create a metadata key for
     * @return The prefixed metadata key
     */
    public static String createMetaKey(String term) {
        return META + term;
    }
    
    /**
     * Creates a key for storing term positions.
     * @param term The term to create a positions key for
     * @return The prefixed positions key
     */
    public static String createPositionsKey(String term) {
        return POSITIONS + term;
    }
    
    /**
     * Extracts the term from a prefixed key.
     * @param key The prefixed key
     * @return The original term
     */
    public static String extractTerm(String key) {
        if (key.startsWith(META)) {
            return key.substring(META.length());
        } else if (key.startsWith(POSITIONS)) {
            return key.substring(POSITIONS.length());
        }
        return key; // Return as-is if no prefix found
    }
} 