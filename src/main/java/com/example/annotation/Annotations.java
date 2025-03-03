package com.example.annotation;

import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.semgraph.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.*;
import me.tongfei.progressbar.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.file.Path;

public class Annotations {
    private static final Logger logger = LoggerFactory.getLogger(Annotations.class);
    private final Path dbFile;
    private final int batchSize;
    private final int threads;
    private final boolean overwrite;
    private final StanfordCoreNLP pipeline;

    public Annotations(Path dbFile, int batchSize, int threads, boolean overwrite) {
        this.dbFile = dbFile;
        this.batchSize = batchSize;
        this.threads = threads;
        this.overwrite = overwrite;
        
        // Create optimized CoreNLP configuration
        CoreNLPConfig config = new CoreNLPConfig(threads);
        this.pipeline = config.createPipeline();
        logger.info("Created CoreNLP pipeline with optimized configuration");
    }

    public void processDocuments() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile)) {
            createTables(conn, overwrite);
            String query = buildQuery(overwrite, null);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                while (rs.next()) {
                    int documentId = rs.getInt("document_id");
                    String text = rs.getString("text");
                    
                    AnnotationResult result = processTextWithCoreNLP(pipeline, text, documentId);
                    insertData(conn, result.annotations, result.dependencies);
                }
            }
        }
    }

    private static class AnnotationResult {
        final List<Map<String, Object>> annotations;
        final List<Map<String, Object>> dependencies;

        AnnotationResult(List<Map<String, Object>> annotations, List<Map<String, Object>> dependencies) {
            this.annotations = annotations;
            this.dependencies = dependencies;
        }
    }

    private static void createTables(Connection conn, boolean overwrite) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            if (overwrite) {
                stmt.execute("DROP TABLE IF EXISTS annotations");
                stmt.execute("DROP TABLE IF EXISTS dependencies");
            }
            stmt.execute("DROP TABLE IF EXISTS index_table");
            stmt.execute("PRAGMA foreign_keys = ON");

            stmt.execute("""
                        CREATE TABLE IF NOT EXISTS annotations (
                            annotation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            document_id INTEGER NOT NULL,
                            sentence_id INTEGER,
                            begin_char INTEGER,
                            end_char INTEGER,
                            token TEXT,
                            lemma TEXT,
                            pos TEXT,
                            ner TEXT,
                            normalized_ner TEXT,
                            FOREIGN KEY (document_id) REFERENCES documents(document_id)
                        )
                    """);

            stmt.execute("""
                        CREATE TABLE IF NOT EXISTS dependencies (
                            dependency_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            document_id INTEGER NOT NULL,
                            sentence_id INTEGER,
                            begin_char INTEGER,
                            end_char INTEGER,
                            head_token TEXT,
                            dependent_token TEXT,
                            relation TEXT,
                            FOREIGN KEY (document_id) REFERENCES documents(document_id)
                        )
                    """);
        }
    }

    /**
     * Get default properties for CoreNLP pipeline
     * 
     * @return Properties configured for our NLP processing requirements
     */
    private static Properties getDefaultCoreNLPProperties() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,depparse");
        props.setProperty("ner.applyNumericClassifiers", "true");
        props.setProperty("ner.useSUTime", "true");
        return props;
    }

    /**
     * Processes text with CoreNLP pipeline, chunking if needed, and returns annotations.
     * 
     * @param pipeline The CoreNLP pipeline to use for processing
     * @param text The text to process
     * @param documentId The ID of the document being processed
     * @return The AnnotationResult containing annotations and dependencies
     */
    private static AnnotationResult processTextWithCoreNLP(StanfordCoreNLP pipeline, String text, int documentId) {
        List<Map<String, Object>> annotations = new ArrayList<>();
        List<Map<String, Object>> dependencies = new ArrayList<>();
        
        // Split text into manageable chunks and track their positions
        TextSegmenter segmenter = new TextSegmenter();
        TextSegmenter.ChunkResult chunkResult = segmenter.chunkDocumentWithPositions(text);
        List<String> chunks = chunkResult.getChunks();
        
        logger.debug("Processing document {} with {} chunks", documentId, chunks.size());
        
        // Keep track of the global sentence ID across all chunks
        AtomicInteger globalSentenceId = new AtomicInteger(0);
        
        // Process each chunk
        for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
            String chunk = chunks.get(chunkIndex);
            CoreDocument document = new CoreDocument(chunk);
            pipeline.annotate(document);
            
            // Get the precise character offset for this chunk in the original document
            int chunkStartOffset = chunkResult.getStartPosition(chunkIndex);
            
            int sentenceId = globalSentenceId.get();
            for (CoreSentence sentence : document.sentences()) {
                List<CoreLabel> tokens = sentence.tokens();
                
                // Skip sentences that are likely duplicates from overlap regions
                if (isOverlapSentence(chunk, sentence, chunkIndex, chunkIndex == chunks.size() - 1)) {
                    continue;
                }
                
                // Process tokens
                for (CoreLabel token : tokens) {
                    Map<String, Object> annotation = new HashMap<>();
                    annotation.put("document_id", documentId);
                    annotation.put("sentence_id", sentenceId);
                    annotation.put("begin_char", token.beginPosition() + chunkStartOffset);
                    annotation.put("end_char", token.endPosition() + chunkStartOffset);
                    annotation.put("token", token.word());
                    annotation.put("lemma", token.lemma());
                    annotation.put("pos", token.tag());
                    annotation.put("ner", token.ner());
                    annotation.put("normalized_ner",
                            token.get(CoreAnnotations.NormalizedNamedEntityTagAnnotation.class));

                    annotations.add(annotation);
                }

                // Process dependencies
                SemanticGraph dependencies_graph = sentence.dependencyParse();
                if (dependencies_graph != null) {
                    for (SemanticGraphEdge edge : dependencies_graph.edgeIterable()) {
                        IndexedWord source = edge.getSource();
                        IndexedWord target = edge.getTarget();

                        int beginChar = Math.min(source.beginPosition(), target.beginPosition()) + chunkStartOffset;
                        int endChar = Math.max(source.endPosition(), target.endPosition()) + chunkStartOffset;

                        Map<String, Object> dependency = new HashMap<>();
                        dependency.put("document_id", documentId);
                        dependency.put("sentence_id", sentenceId);
                        dependency.put("begin_char", beginChar);
                        dependency.put("end_char", endChar);
                        dependency.put("source_token", source.word());
                        dependency.put("source_begin", source.beginPosition() + chunkStartOffset);
                        dependency.put("source_end", source.endPosition() + chunkStartOffset);
                        dependency.put("target_token", target.word());
                        dependency.put("target_begin", target.beginPosition() + chunkStartOffset);
                        dependency.put("target_end", target.endPosition() + chunkStartOffset);
                        dependency.put("dep_type", edge.getRelation().toString());

                        dependencies.add(dependency);
                    }
                }
                sentenceId++;
            }
            
            globalSentenceId.set(sentenceId);
        }
        
        return new AnnotationResult(annotations, dependencies);
    }
    
    /**
     * Checks if a sentence is likely from an overlap region by checking if it starts
     * near the beginning of a non-first chunk or ends near the end of a non-last chunk.
     * 
     * @param chunk The current text chunk being processed
     * @param sentence The CoreNLP sentence being considered
     * @param chunkIndex The index of the current chunk in the sequence of chunks
     * @param isLastChunk Whether this is the last chunk in the document
     * @return true if the sentence appears to be from an overlap region
     */
    private static boolean isOverlapSentence(String chunk, CoreSentence sentence, 
                                            int chunkIndex, boolean isLastChunk) {
        List<CoreLabel> tokens = sentence.tokens();
        if (tokens.isEmpty()) {
            return false;
        }
        
        int sentenceStart = tokens.get(0).beginPosition();
        int sentenceEnd = tokens.get(tokens.size() - 1).endPosition();
        
        // For non-first chunks, check if the sentence starts in the overlap region
        // Since we add overlap at the beginning of chunks, sentences that start in
        // the first OVERLAP_SIZE characters are likely from the previous chunk
        if (chunkIndex > 0 && sentenceStart < TextSegmenter.OVERLAP_SIZE) {
            return true;
        }
        
        // For non-last chunks, we don't need to check the end because the next chunk
        // will contain the complete sentence in its overlap region
        
        return false;
    }

    private static void insertData(Connection conn, List<Map<String, Object>> annotations,
            List<Map<String, Object>> dependencies) throws SQLException {
        String annotationSQL = """
                    INSERT INTO annotations (
                        document_id, sentence_id, begin_char, end_char, token,
                        lemma, pos, ner, normalized_ner
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        String dependencySQL = """
                    INSERT INTO dependencies (
                        document_id, sentence_id, begin_char, end_char,
                        head_token, dependent_token, relation
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """;

        try (PreparedStatement annotationStmt = conn.prepareStatement(annotationSQL);
                PreparedStatement dependencyStmt = conn.prepareStatement(dependencySQL)) {

            // Use larger batch sizes for better performance
            annotationStmt.setFetchSize(1000);
            dependencyStmt.setFetchSize(1000);

            for (Map<String, Object> annotation : annotations) {
                annotationStmt.setInt(1, (Integer) annotation.get("document_id"));
                annotationStmt.setInt(2, (Integer) annotation.get("sentence_id"));
                annotationStmt.setInt(3, (Integer) annotation.get("begin_char"));
                annotationStmt.setInt(4, (Integer) annotation.get("end_char"));
                annotationStmt.setString(5, (String) annotation.get("token"));
                annotationStmt.setString(6, (String) annotation.get("lemma"));
                annotationStmt.setString(7, (String) annotation.get("pos"));
                annotationStmt.setString(8, (String) annotation.get("ner"));
                annotationStmt.setString(9, (String) annotation.get("normalized_ner"));

                annotationStmt.addBatch();
            }
            annotationStmt.executeBatch();

            for (Map<String, Object> dependency : dependencies) {
                dependencyStmt.setInt(1, (Integer) dependency.get("document_id"));
                dependencyStmt.setInt(2, (Integer) dependency.get("sentence_id"));
                dependencyStmt.setInt(3, (Integer) dependency.get("begin_char"));
                dependencyStmt.setInt(4, (Integer) dependency.get("end_char"));
                dependencyStmt.setString(5, (String) dependency.get("head_token"));
                dependencyStmt.setString(6, (String) dependency.get("dependent_token"));
                dependencyStmt.setString(7, (String) dependency.get("relation"));

                dependencyStmt.addBatch();
            }
            dependencyStmt.executeBatch();
        }
    }

    private static String buildQuery(boolean overwrite, Integer limit) {
        StringBuilder query = new StringBuilder("SELECT document_id, text FROM documents");

        if (!overwrite) {
            query.append(" WHERE document_id NOT IN (SELECT DISTINCT document_id FROM annotations)");
        }

        if (limit != null) {
            query.append(" LIMIT ").append(limit);
        }

        return query.toString();
    }

    private static void processDatabase(String dbFile, int batchSize, boolean overwrite,
            int threads, Integer limit) throws SQLException {
        logger.info("Starting database processing with {} threads", threads);
        
        // Create optimized CoreNLP configuration
        CoreNLPConfig config = new CoreNLPConfig(threads);
        StanfordCoreNLP pipeline = config.createPipeline();
        logger.info("Created CoreNLP pipeline with optimized configuration");

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile)) {
            // Enable WAL mode for better concurrent access
            try (Statement pragma = conn.createStatement()) {
                pragma.execute("PRAGMA journal_mode=WAL");
                pragma.execute("PRAGMA synchronous=NORMAL");
                pragma.execute("PRAGMA temp_store=MEMORY");
                pragma.execute("PRAGMA cache_size=-2000"); // Use 2GB cache
                logger.debug("Configured SQLite optimizations");
            }

            conn.setAutoCommit(false); // Enable transaction mode
            createTables(conn, overwrite);

            String countQuery = "SELECT COUNT(*) FROM (" + buildQuery(overwrite, limit) + ")";
            int totalDocuments;

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(countQuery)) {
                totalDocuments = rs.getInt(1);
            }
            logger.info("Found {} documents to process", totalDocuments);

            int processedDocuments = 0;
            int batchCount = 0;
            List<Map<String, Object>> batchAnnotations = new ArrayList<>();
            List<Map<String, Object>> batchDependencies = new ArrayList<>();

            try (ProgressBar pb = new ProgressBar("Processing articles", totalDocuments)) {
                String query = buildQuery(overwrite, limit);

                try (Statement readStmt = conn.createStatement()) {
                    readStmt.setFetchSize(batchSize);
                    ResultSet rs = readStmt.executeQuery(query);

                    while (rs.next() && processedDocuments < totalDocuments) {
                        int documentId = rs.getInt("document_id");
                        String text = rs.getString("text");

                        AnnotationResult result = processTextWithCoreNLP(pipeline, text, documentId);
                        batchAnnotations.addAll(result.annotations);
                        batchDependencies.addAll(result.dependencies);

                        batchCount++;
                        processedDocuments++;
                        pb.step();

                        // Batch insert when we reach batch size
                        if (batchCount >= batchSize) {
                            insertData(conn, batchAnnotations, batchDependencies);
                            conn.commit();
                            batchAnnotations.clear();
                            batchDependencies.clear();
                            batchCount = 0;
                        }
                    }

                    // Insert any remaining items in the batch
                    if (!batchAnnotations.isEmpty()) {
                        insertData(conn, batchAnnotations, batchDependencies);
                        conn.commit();
                    }
                }
            }

            System.out.printf("%nProcessed %d articles.%n", processedDocuments);
        }
    }

    public static void main(String[] args) {
        // Parse command line arguments using argparse4j
        ArgumentParser parser = ArgumentParsers.newFor("Annotations").build()
                .defaultHelp(true)
                .description("Annotate existing SQLite database with CoreNLP");

        parser.addArgument("-d", "--db")
                .required(true)
                .help("SQLite database file path");

        parser.addArgument("-b", "--batch_size")
                .setDefault(500)
                .type(Integer.class)
                .help("Batch size for processing (default: 500)");

        parser.addArgument("-o", "--overwrite")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Overwrite existing annotations (default: False)");

        parser.addArgument("-t", "--threads")
                .setDefault(8)
                .type(Integer.class)
                .help("Number of threads for CoreNLP (default: 8)");

        parser.addArgument("-l", "--limit")
                .type(Integer.class)
                .help("Limit the number of documents to process (default: None)");

        try {
            Namespace ns = parser.parseArgs(args);

            processDatabase(
                    ns.getString("db"),
                    ns.getInt("batch_size"),
                    ns.getBoolean("overwrite"),
                    ns.getInt("threads"),
                    ns.get("limit"));

            System.out.printf("Processing complete. Data stored in database: %s%n", ns.getString("db"));

        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error processing database: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
