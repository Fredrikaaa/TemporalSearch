package com.example.nlp;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.junit.jupiter.api.Test;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for CoreNLPConfig
 */
class CoreNLPConfigTest {

    @Test
    void testDefaultConfiguration() {
        CoreNLPConfig config = new CoreNLPConfig();
        Properties props = config.getProperties();
        
        // Test core annotators
        assertEquals("tokenize,ssplit,pos,lemma,ner,depparse", props.getProperty("annotators"));
        assertEquals(String.valueOf(Runtime.getRuntime().availableProcessors()), 
                    props.getProperty("threads"));
        
        // Test model selections
        assertEquals("edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger", 
                    props.getProperty("pos.model"));
        assertEquals("edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz", 
                    props.getProperty("ner.model"));
        assertEquals("edu/stanford/nlp/models/parser/nndep/english_RB.gz", 
                    props.getProperty("depparse.model"));
        
        // Test performance optimizations
        assertEquals("false", props.getProperty("ner.useSUTime"));
        assertEquals("false", props.getProperty("ner.applyNumericClassifiers"));
        assertEquals("false", props.getProperty("ner.applyFineGrained"));
        
        // Test length constraints
        assertEquals("120", props.getProperty("pos.maxlen"));
        assertEquals("120", props.getProperty("ner.maxlen"));
        assertEquals("120", props.getProperty("depparse.maxlen"));
        assertEquals("120", props.getProperty("parse.maxlen"));
        assertEquals("50", props.getProperty("parse.maxsubsentencelength"));
    }
    
    @Test
    void testCustomThreadConfiguration() {
        int customThreads = 4;
        CoreNLPConfig config = new CoreNLPConfig(customThreads);
        Properties props = config.getProperties();
        
        assertEquals(String.valueOf(customThreads), props.getProperty("threads"));
    }
    
    @Test
    void testTokenizerConfiguration() {
        CoreNLPConfig config = new CoreNLPConfig();
        Properties props = config.getProperties();
        
        String tokenizeOptions = props.getProperty("tokenize.options");
        assertTrue(tokenizeOptions.contains("normalizeParentheses=true"));
        assertTrue(tokenizeOptions.contains("normalizeOtherBrackets=true"));
        assertTrue(tokenizeOptions.contains("ptb3Escaping=false"));
        assertTrue(tokenizeOptions.contains("invertible=true"));
    }
    
    @Test
    void testSentenceSplitterConfiguration() {
        CoreNLPConfig config = new CoreNLPConfig();
        Properties props = config.getProperties();
        
        assertEquals("[.!?]|\\n\\n+", props.getProperty("ssplit.boundaryTokenRegex"));
        assertEquals("two", props.getProperty("ssplit.newlineIsSentenceBreak"));
        assertEquals("false", props.getProperty("tokenize.tokenizeNLs"));
    }
} 