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
import java.nio.file.Path;

public class Annotations {
    private static final Logger logger = LoggerFactory.getLogger(Annotations.class);
    private final Path dbFile;
    private final boolean overwrite;
    private final Integer limit;
    private final StanfordCoreNLP pipeline;
    
    public Annotations(Path dbFile, int threads, boolean overwrite, Integer limit) {
        this.dbFile = dbFile;
        this.overwrite = overwrite;
        this.limit = limit;
        
        // Create optimized CoreNLP configuration
        CoreNLPConfig config = new CoreNLPConfig(threads);
        this.pipeline = config.createPipeline();
        logger.info("Created CoreNLP pipeline with optimized configuration");
    }

    public void processDocuments() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile)) {
            createTables(conn, overwrite);
            
            String query = buildQuery(overwrite, limit);
            
            // First count total documents to process for progress tracking
            int totalDocuments = 0;
            try (Statement countStmt = conn.createStatement();
                 ResultSet countRs = countStmt.executeQuery("SELECT COUNT(*) FROM (" + query + ")")) {
                if (countRs.next()) {
                    totalDocuments = countRs.getInt(1);
                }
            }
            
            logger.info("Found {} documents to process", totalDocuments);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query);
                 ProgressBar pb = new ProgressBar("Processing documents", totalDocuments)) {
                
                int processed = 0;
                while (rs.next()) {
                    int documentId = rs.getInt("document_id");
                    String text = rs.getString("text");
                    
                    AnnotationResult result = processTextWithCoreNLP(pipeline, text, documentId);
                    insertData(conn, result.annotations, result.dependencies);
                    
                    pb.step();
                    processed++;
                    
                    if (processed % 10 == 0) {
                        pb.setExtraMessage(String.format("(%d/%d)", processed, totalDocuments));
                    }
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
        
        logger.debug("Processing document {}", documentId);
        
        // Process the document directly without chunking
        CoreDocument document = new CoreDocument(text);
        pipeline.annotate(document);
        
        int sentenceId = 0;
        for (CoreSentence sentence : document.sentences()) {
            List<CoreLabel> tokens = sentence.tokens();
            
            // Process tokens
            for (CoreLabel token : tokens) {
                Map<String, Object> annotation = new HashMap<>();
                annotation.put("document_id", documentId);
                annotation.put("sentence_id", sentenceId);
                annotation.put("begin_char", token.beginPosition());
                annotation.put("end_char", token.endPosition());
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

                    int beginChar = Math.min(source.beginPosition(), target.beginPosition());
                    int endChar = Math.max(source.endPosition(), target.endPosition());

                    Map<String, Object> dependency = new HashMap<>();
                    dependency.put("document_id", documentId);
                    dependency.put("sentence_id", sentenceId);
                    dependency.put("begin_char", beginChar);
                    dependency.put("end_char", endChar);
                    dependency.put("head_token", source.word());
                    dependency.put("dependent_token", target.word());
                    dependency.put("relation", edge.getRelation().toString());

                    dependencies.add(dependency);
                }
            }
            sentenceId++;
        }
        
        return new AnnotationResult(annotations, dependencies);
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
            annotationStmt.setFetchSize(10000);
            dependencyStmt.setFetchSize(10000);

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

    public static void main(String[] args) {
        // Create parser with required and optional flags
        ArgumentParser parser = ArgumentParsers.newFor("Annotations").build()
                .defaultHelp(true)
                .description("Annotate existing SQLite database with CoreNLP");

        parser.addArgument("-d", "--db")
                .required(true)
                .help("SQLite database file path");

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
            
            // Create a single instance of Annotations with the pipeline
            Annotations annotations = new Annotations(
                Path.of(ns.getString("db")),
                ns.getInt("threads"),
                ns.getBoolean("overwrite"),
                ns.getInt("limit")
            );
            
            // Use the instance method instead of static method
            annotations.processDocuments();

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
