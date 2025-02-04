package com.example.annotation;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.junit.jupiter.api.Test;
import java.util.Properties;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for CoreNLPConfig
 */
class CoreNLPConfigTest {

    @Test
    void testRequiredAnnotatorsPresent() {
        CoreNLPConfig config = new CoreNLPConfig();
        Properties props = config.getProperties();
        
        String annotators = props.getProperty("annotators");
        assertNotNull(annotators, "Annotators should be configured");
        
        List<String> requiredAnnotators = Arrays.asList(
            "tokenize", "ssplit", "pos", "lemma", "ner", "parse"
        );
        
        List<String> configuredAnnotators = Arrays.asList(annotators.split(","));
        assertTrue(configuredAnnotators.containsAll(requiredAnnotators),
            "All required annotators should be present");
    }
    
    @Test
    void testRequiredModelsConfigured() {
        CoreNLPConfig config = new CoreNLPConfig();
        Properties props = config.getProperties();
        
        // Check that model paths are configured
        assertNotNull(props.getProperty("pos.model"), "POS model should be configured");
        assertNotNull(props.getProperty("ner.model"), "NER model should be configured");
        assertNotNull(props.getProperty("parse.model"), "Parser model should be configured");
        
        // Verify we're using shift-reduce parser
        assertTrue(props.getProperty("parse.model").contains("srparser"),
            "Should use shift-reduce parser for speed");
    }
    
    @Test
    void testCustomThreadConfiguration() {
        int customThreads = 4;
        CoreNLPConfig config = new CoreNLPConfig(customThreads);
        Properties props = config.getProperties();
        
        assertEquals(String.valueOf(customThreads), props.getProperty("threads"),
            "Thread count should match configured value");
        assertEquals(String.valueOf(customThreads), props.getProperty("parse.nthreads"),
            "Parser thread count should match configured value");
        assertEquals(String.valueOf(customThreads), props.getProperty("ner.nthreads"),
            "NER thread count should match configured value");
    }
    
    @Test
    void testTokenizerRequiredFeatures() {
        CoreNLPConfig config = new CoreNLPConfig();
        Properties props = config.getProperties();
        
        String tokenizeOptions = props.getProperty("tokenize.options");
        assertNotNull(tokenizeOptions, "Tokenizer options should be configured");
        
        // Only test for critical features
        assertTrue(tokenizeOptions.contains("invertible=true"),
            "Tokenizer must support character offsets");
    }
    
    @Test
    void testSentenceSplitterConfigured() {
        CoreNLPConfig config = new CoreNLPConfig();
        Properties props = config.getProperties();
        
        assertNotNull(props.getProperty("ssplit.boundaryTokenRegex"),
            "Sentence boundary regex should be configured");
        assertNotNull(props.getProperty("ssplit.newlineIsSentenceBreak"),
            "Newline handling should be configured");
    }
    
    @Test
    void testPipelineCreation() {
        CoreNLPConfig config = new CoreNLPConfig();
        StanfordCoreNLP pipeline = config.createPipeline();
        assertNotNull(pipeline, "Should create a valid pipeline instance");
    }
    
    @Test
    void testSpeedOptimizationsEnabled() {
        CoreNLPConfig config = new CoreNLPConfig();
        Properties props = config.getProperties();
        
        // Verify speed-critical settings
        assertEquals("AGGRESSIVE", props.getProperty("memoryUsage"),
            "Should use aggressive memory mode for speed");
        assertEquals("false", props.getProperty("ner.applyFineGrained"),
            "Should disable expensive NER features");
        assertEquals("false", props.getProperty("ner.useNGrams"),
            "Should disable NGram features for speed");
        assertTrue(Boolean.parseBoolean(props.getProperty("parse.binaryTrees")),
            "Should enable binary trees for SR parser");
    }
    
    @Test
    void testLengthConstraintsConfigured() {
        CoreNLPConfig config = new CoreNLPConfig();
        Properties props = config.getProperties();
        
        // Check that length constraints exist
        assertNotNull(props.getProperty("pos.maxlen"), "POS length constraint should be configured");
        assertNotNull(props.getProperty("ner.maxlen"), "NER length constraint should be configured");
        assertNotNull(props.getProperty("parse.maxlen"), "Parse length constraint should be configured");
    }
} 