package com.example.annotation;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/**
 * Configuration class for CoreNLP pipeline with optimized settings.
 * This class encapsulates all CoreNLP-related configuration and provides
 * factory methods for creating optimized pipeline instances.
 */
public class CoreNLPConfig {
    private static final Logger logger = LoggerFactory.getLogger(CoreNLPConfig.class);
    
    // Default thread count if not specified
    private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();
    
    // Maximum lengths for different components to prevent OOM
    private static final int MAX_SENTENCE_LENGTH = 120;
    
    // Model paths
    private static final String SR_PARSER_MODEL = "stanford-english-extra-corenlp-models-current/edu/stanford/nlp/models/srparser/englishSR.ser.gz";
    private static final String FALLBACK_PARSER_MODEL = "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz";
    
    private final Properties properties;
    
    /**
     * Creates a new CoreNLPConfig with optimized settings
     * @param threads Number of threads to use for parallel processing
     */
    public CoreNLPConfig(int threads) {
        this.properties = createOptimizedProperties(threads);
        logger.info("Initialized CoreNLP configuration with {} threads", threads);
    }
    
    /**
     * Creates a new CoreNLPConfig with default thread count
     */
    public CoreNLPConfig() {
        this(DEFAULT_THREADS);
    }
    
    /**
     * Creates and returns a new StanfordCoreNLP pipeline instance with the optimized configuration
     * @return A configured StanfordCoreNLP pipeline instance
     */
    public StanfordCoreNLP createPipeline() {
        logger.debug("Creating new CoreNLP pipeline with optimized configuration");
        try {
            if (new java.io.File(SR_PARSER_MODEL).exists()) {
                logger.info("Using shift-reduce parser model from: {}", SR_PARSER_MODEL);
            } else {
                logger.warn("Shift-reduce parser model not found at: {}. Using fallback parser.", SR_PARSER_MODEL);
                properties.setProperty("parse.model", FALLBACK_PARSER_MODEL);
            }
            return new StanfordCoreNLP(properties);
        } catch (Exception e) {
            logger.warn("Failed to load parser model. Using fallback parser: {}", e.getMessage());
            properties.setProperty("parse.model", FALLBACK_PARSER_MODEL);
            return new StanfordCoreNLP(properties);
        }
    }
    
    /**
     * Creates optimized properties for the CoreNLP pipeline
     * @param threads Number of threads to use
     * @return Properties configured for optimal performance
     */
    private static Properties createOptimizedProperties(int threads) {
        Properties props = new Properties();
        
        // Core annotators - only what we actually use in Annotations.java
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse");
        props.setProperty("threads", String.valueOf(threads));
        
        // Model selection for speed optimization
        props.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger"); // Faster model
        props.setProperty("ner.model", "edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz"); // Faster 3-class model
        props.setProperty("parse.model", SR_PARSER_MODEL); // Shift-reduce parser - 30x faster
        
        // Parser specific settings
        props.setProperty("parse.maxlen", String.valueOf(MAX_SENTENCE_LENGTH));
        props.setProperty("parse.binaryTrees", "true");  // Required for SR parser
        
        // Rich feature set for NER - focused on speed
        props.setProperty("ner.useSUTime", "true");                // Required for normalized_ner
        props.setProperty("ner.applyNumericClassifiers", "true");  // Required for normalized_ner
        props.setProperty("ner.applyFineGrained", "false");        // Disable for speed
        props.setProperty("ner.useNGrams", "false");               // Disable for speed
        props.setProperty("ner.buildEntityMentions", "false");     // Default setting for compatibility
        
        // Memory settings - optimized for speed
        props.setProperty("memoryUsage", "AGGRESSIVE");            // Prefer speed over memory
        props.setProperty("maxAdditionalKnownLCWords", "80000");   // Large case handling cache
        
        // Length constraints - balanced for speed
        props.setProperty("pos.maxlen", String.valueOf(MAX_SENTENCE_LENGTH));
        props.setProperty("ner.maxlen", String.valueOf(MAX_SENTENCE_LENGTH));
        props.setProperty("depparse.maxlen", String.valueOf(MAX_SENTENCE_LENGTH));
        
        // Enhanced tokenizer settings - optimized for speed
        props.setProperty("tokenize.options", String.join(",",
            "normalizeParentheses=true",
            "normalizeOtherBrackets=true",
            "ptb3Escaping=false",          // Faster processing
            "invertible=true",             // Required for char offsets
            "untokenizable=noneKeep"       // Skip problematic tokens
        ));
        
        // Simple sentence splitting for speed
        props.setProperty("ssplit.boundaryTokenRegex", "[.!?]+");  // Default boundary regex
        props.setProperty("ssplit.newlineIsSentenceBreak", "two");
        props.setProperty("tokenize.tokenizeNLs", "true");       // Handle newlines properly
        
        // Thread allocation - optimized for parallel processing
        props.setProperty("parse.nthreads", String.valueOf(threads));
        props.setProperty("ner.nthreads", String.valueOf(threads));
        
        // Additional NER features - minimal for speed
        props.setProperty("ner.combinationMode", "NORMAL");       // Required for consistency
        props.setProperty("ner.usePrecedenceList", "false");      // Disable for speed
        props.setProperty("ner.applyChunking", "false");         // Disable for speed
        
        // Optimization for batch processing
        props.setProperty("nthreads", String.valueOf(threads));   // Global thread setting
        props.setProperty("output.goldConll", "false");          // Disable unused output
        props.setProperty("output.prettyPrint", "false");        // Disable unused output
        
        return props;
    }
    
    /**
     * Gets the underlying properties
     * @return The CoreNLP properties
     */
    public Properties getProperties() {
        return new Properties(properties);
    }
} 