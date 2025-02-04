package com.example.nlp;

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
    private static final int MAX_PARSE_LENGTH = 50;
    
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
        return new StanfordCoreNLP(properties);
    }
    
    /**
     * Creates optimized properties for the CoreNLP pipeline
     * @param threads Number of threads to use
     * @return Properties configured for optimal performance
     */
    private static Properties createOptimizedProperties(int threads) {
        Properties props = new Properties();
        
        // Core annotators configuration
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,depparse");
        props.setProperty("threads", String.valueOf(threads));
        
        // Model selection for optimal performance
        props.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger");
        props.setProperty("ner.model", "edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz");
        props.setProperty("depparse.model", "edu/stanford/nlp/models/parser/nndep/english_RB.gz");
        
        // Performance optimizations
        props.setProperty("ner.useSUTime", "false");
        props.setProperty("ner.applyNumericClassifiers", "false");
        props.setProperty("ner.applyFineGrained", "false");
        
        // Length constraints to prevent OOM
        props.setProperty("pos.maxlen", String.valueOf(MAX_SENTENCE_LENGTH));
        props.setProperty("ner.maxlen", String.valueOf(MAX_SENTENCE_LENGTH));
        props.setProperty("depparse.maxlen", String.valueOf(MAX_SENTENCE_LENGTH));
        props.setProperty("parse.maxlen", String.valueOf(MAX_SENTENCE_LENGTH));
        props.setProperty("parse.maxsubsentencelength", String.valueOf(MAX_PARSE_LENGTH));
        
        // Tokenizer optimizations
        props.setProperty("tokenize.options", String.join(",",
            "normalizeParentheses=true",
            "normalizeOtherBrackets=true",
            "ptb3Escaping=false",
            "invertible=true"
        ));
        
        // Sentence splitting configuration
        props.setProperty("ssplit.boundaryTokenRegex", "[.!?]|\\n\\n+");
        props.setProperty("ssplit.newlineIsSentenceBreak", "two");
        props.setProperty("tokenize.tokenizeNLs", "false");
        
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