package com.example;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import com.example.index.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LevelDBBrowser {
    private static final String DELIMITER = "\u0000";
    private static final String WILDCARD = "*";
    private static final Logger logger = LoggerFactory.getLogger(LevelDBBrowser.class);

    public static void main(String[] args) throws IOException {
        logger.debug("Starting LevelDBBrowser...");
        ArgumentParser parser = ArgumentParsers.newFor("LevelDBBrowser").build()
                .defaultHelp(true)
                .description("Browse contents of LevelDB index databases");

        parser.addArgument("index_type")
                .choices("unigram", "bigram", "trigram", "dependency")
                .help("Type of index to browse");

        parser.addArgument("db_path")
                .help("Base path to index directory");

        parser.addArgument("-w", "--words")
                .nargs("+")
                .help("Look up specific word(s). Use 1-3 words based on index type.");

        parser.addArgument("-l", "--list")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("List all entries in the index");

        parser.addArgument("-c", "--count")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Show occurrence counts");

        parser.addArgument("-t", "--time")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Show temporal distribution of occurrences");

        try {
            Namespace ns = parser.parseArgs(args);
            String indexType = ns.getString("index_type");
            String basePath = ns.getString("db_path");
            List<String> words = ns.getList("words");
            boolean listEntries = ns.getBoolean("list");
            boolean showCounts = ns.getBoolean("count");
            boolean showTime = ns.getBoolean("time");

            String dbPath = basePath + "/" + indexType;

            // Validate word count for dependency index
            if (words != null && indexType.equals("dependency")) {
                if (words.size() < 1 || words.size() > 3) {
                    System.err.println("Error: dependency index requires 1-3 components (head_token [relation] [dependent_token])");
                    System.exit(1);
                }
            } else if (words != null) {
                // Existing validation for n-grams
                int expectedWords = getExpectedWordCount(indexType);
                if (words.size() != expectedWords) {
                    System.err.printf("Error: %s index requires exactly %d word(s)%n",
                            indexType, expectedWords);
                    System.exit(1);
                }
            }

            // Open the database
            Options options = new Options();
            try (DB db = factory.open(new File(dbPath), options)) {
                if (words != null) {
                    lookupWords(db, words, indexType, showTime);
                }

                if (listEntries || showCounts) {
                    listEntries(db, indexType, showCounts);
                }
            }
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error browsing database: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static int getExpectedWordCount(String indexType) {
        return switch (indexType) {
            case "unigram" -> 1;
            case "bigram" -> 2;
            case "trigram" -> 3;
            case "dependency" -> -1; // Special handling for dependencies
            default -> throw new IllegalArgumentException("Invalid index type");
        };
    }

    private static void lookupWords(DB db, List<String> words, String indexType, boolean showTime) throws IOException {
        if (indexType.equals("dependency")) {
            lookupDependency(db, words, showTime);
            return;
        }
        // Create lookup key based on index type
        String key = String.join(DELIMITER, words.stream()
                .map(String::toLowerCase)
                .toList());

        byte[] data = db.get(bytes(key));
        if (data == null) {
            System.out.printf("%s not found in index%n",
                    formatSearchTerm(words, indexType));
            return;
        }

        PositionList positions = PositionList.deserialize(data);
        System.out.printf("Found %s in %d positions:%n",
                formatSearchTerm(words, indexType), positions.size());

        // Sort positions by date if showing temporal distribution
        if (showTime) {
            positions.getPositions().sort((a, b) -> a.getTimestamp().compareTo(b.getTimestamp()));
        }

        // Group positions by timestamp if showing temporal distribution
        if (showTime) {
            Map<String, Integer> timeDistribution = new TreeMap<>();
            for (Position pos : positions.getPositions()) {
                String yearMonth = pos.getTimestamp().toString().substring(0, 7); // YYYY-MM
                timeDistribution.merge(yearMonth, 1, Integer::sum);
            }

            System.out.println("\nTemporal distribution:");
            timeDistribution.forEach((date, count) -> System.out.printf("%s: %d occurrences%n", date, count));
        } else {
            // Show individual positions
            for (Position pos : positions.getPositions()) {
                System.out.printf("  Document %d, Sentence %d, Chars %d-%d, Date: %s%n",
                        pos.getDocumentId(), pos.getSentenceId(),
                        pos.getBeginPosition(), pos.getEndPosition(),
                        pos.getTimestamp());
            }
        }
    }

    private static String formatSearchTerm(List<String> words, String indexType) {
        return switch (indexType) {
            case "unigram" -> String.format("word '%s'", words.get(0));
            case "bigram" -> String.format("phrase '%s %s'", words.get(0), words.get(1));
            case "trigram" -> String.format("phrase '%s %s %s'",
                    words.get(0), words.get(1), words.get(2));
            default -> throw new IllegalArgumentException("Invalid index type");
        };
    }

    private static void lookupDependency(DB db, List<String> pattern, boolean showTime) throws IOException {
        String headToken = pattern.size() > 0 ? pattern.get(0).toLowerCase() : WILDCARD;
        String relation = pattern.size() > 1 ? pattern.get(1).toLowerCase() : WILDCARD;
        String depToken = pattern.size() > 2 ? pattern.get(2).toLowerCase() : WILDCARD;

        String searchPattern = String.join(DELIMITER, headToken, relation, depToken);
        logger.debug("Looking up dependency pattern: {}", searchPattern);
        
        Map<String, PositionList> matches = new HashMap<>();
        
        try (DBIterator iterator = db.iterator()) {
            for (iterator.seek(bytes(searchPattern)); iterator.hasNext(); iterator.next()) {
                String key = asString(iterator.peekNext().getKey());
                logger.debug("Examining key: {}", key);
                
                String[] parts = key.split(DELIMITER);
                
                if (parts.length != 3) continue;
                
                if (matchesPattern(parts[0], headToken) &&
                    matchesPattern(parts[1], relation) &&
                    matchesPattern(parts[2], depToken)) {
                    logger.debug("Found matching dependency: {} -{}- {}", parts[0], parts[1], parts[2]);
                    matches.put(key, PositionList.deserialize(iterator.peekNext().getValue()));
                }
               
                // Stop if we've moved past potential matches
                if (!key.startsWith(headToken) && !headToken.equals(WILDCARD)) {
                    break;
                }
            }
        }

        if (matches.isEmpty()) {
            System.out.printf("No matches found for dependency pattern: %s%n",
                    formatDependencyPattern(headToken, relation, depToken));
            return;
        }

        System.out.printf("Found matches for dependency pattern %s:%n",
                formatDependencyPattern(headToken, relation, depToken));

        for (Map.Entry<String, PositionList> entry : matches.entrySet()) {
            String[] parts = entry.getKey().split(DELIMITER);
            PositionList positions = entry.getValue();

            if (showTime) {
                showTemporalDistribution(parts, positions);
            } else {
                showPositions(parts, positions);
            }
        }
    }

    private static boolean matchesPattern(String value, String pattern) {
        return pattern.equals(WILDCARD) || pattern.equals(value);
    }

    private static String formatDependencyPattern(String head, String rel, String dep) {
        return String.format("'%s-%s->%s'", head, rel, dep);
    }

    private static void showTemporalDistribution(String[] parts, PositionList positions) {
        Map<String, Integer> timeDistribution = new TreeMap<>();
        for (Position pos : positions.getPositions()) {
            String yearMonth = pos.getTimestamp().toString().substring(0, 7);
            timeDistribution.merge(yearMonth, 1, Integer::sum);
        }

        System.out.printf("\nDependency: %s-%s->%s%n", parts[0], parts[1], parts[2]);
        System.out.println("Temporal distribution:");
        timeDistribution.forEach((date, count) ->
            System.out.printf("  %s: %d occurrences%n", date, count));
    }

    private static void showPositions(String[] parts, PositionList positions) {
        System.out.printf("\nDependency: %s-%s->%s (%d occurrences)%n",
                parts[0], parts[1], parts[2], positions.size());
        for (Position pos : positions.getPositions()) {
            System.out.printf("  Document %d, Sentence %d, Chars %d-%d, Date: %s%n",
                    pos.getDocumentId(), pos.getSentenceId(),
                    pos.getBeginPosition(), pos.getEndPosition(),
                    pos.getTimestamp());
        }
    }

    private static void listEntries(DB db, String indexType, boolean showCounts) throws IOException {
        Map<String, Integer> entries = new TreeMap<>();
        try (DBIterator iterator = db.iterator()) {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                String key = new String(iterator.peekNext().getKey(), "UTF-8");
                PositionList positions = PositionList.deserialize(iterator.peekNext().getValue());

                // Format key based on index type
                String displayKey = indexType.equals("dependency") ?
                        formatDependencyKey(key) :
                        key.replace(DELIMITER, " ");
                entries.put(displayKey, positions.size());
            }
        }

        if (showCounts) {
            // Sort by count in descending order
            List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(entries.entrySet());
            sortedEntries.sort(Map.Entry.<String, Integer>comparingByValue().reversed());

            System.out.printf("%s counts (sorted by frequency):%n",
                    capitalizeFirst(indexType));
            for (Map.Entry<String, Integer> entry : sortedEntries) {
                System.out.printf("%s: %d occurrences%n", entry.getKey(), entry.getValue());
            }
        } else {
            System.out.printf("%ss in index:%n", indexType);
            for (String key : entries.keySet()) {
                System.out.println(key);
            }
        }
        System.out.printf("%nTotal unique %ss: %d%n", indexType, entries.size());
    }

    private static String formatDependencyKey(String key) {
        String[] parts = key.split(DELIMITER);
        if (parts.length != 3) return key;
        return String.format("%s-%s->%s", parts[0], parts[1], parts[2]);
    }

    private static String capitalizeFirst(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String[] parseKey(String key) {
        // Format: head-relation->dependent
        int arrowIndex = key.indexOf("->");
        if (arrowIndex == -1) return new String[0];
        
        String beforeArrow = key.substring(0, arrowIndex);
        String dependent = key.substring(arrowIndex + 2);
        
        int relationIndex = beforeArrow.indexOf('-');
        if (relationIndex == -1) return new String[0];
        
        String head = beforeArrow.substring(0, relationIndex);
        String relation = beforeArrow.substring(relationIndex + 1);
        
        return new String[]{head, relation, dependent};
    }
}
