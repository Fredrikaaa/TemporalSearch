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
                .choices("unigram", "bigram", "trigram", "dependency", "ner_date", "pos")
                .help("Type of index to browse");

        parser.addArgument("db_path")
                .help("Base path to index directory");

        parser.addArgument("-w", "--words")
                .nargs("+")
                .help("Look up specific word(s) or POS tags. Use 1-3 words based on index type.");

        parser.addArgument("-l", "--list")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("List all entries in the index");

        parser.addArgument("-c", "--count")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Show occurrence counts");

        parser.addArgument("-t", "--time")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Show temporal distribution of occurrences");

        parser.addArgument("-s", "--summary")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Show summary statistics of the index");

        parser.addArgument("-m", "--min-occurrences")
                .type(Integer.class)
                .setDefault(0)
                .help("Minimum number of occurrences required to display an entry");

        try {
            Namespace ns = parser.parseArgs(args);
            String indexType = ns.getString("index_type");
            String basePath = ns.getString("db_path");
            List<String> words = ns.getList("words");
            boolean listEntries = ns.getBoolean("list");
            boolean showCounts = ns.getBoolean("count");
            boolean showTime = ns.getBoolean("time");
            boolean showSummary = ns.getBoolean("summary");
            int minOccurrences = ns.getInt("min_occurrences");

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
                if (showSummary) {
                    showSummary(db, indexType);
                }

                if (words != null) {
                    lookupWords(db, words, indexType, showTime, minOccurrences);
                }

                if (listEntries || showCounts) {
                    listEntries(db, indexType, showCounts, minOccurrences);
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
            case "unigram", "ner_date", "pos" -> 1;
            case "bigram" -> 2;
            case "trigram" -> 3;
            case "dependency" -> 3;
            default -> throw new IllegalArgumentException("Unknown index type: " + indexType);
        };
    }

    private static void lookupWords(DB db, List<String> words, String indexType, boolean showTime, int minOccurrences) throws IOException {
        if (indexType.equals("dependency")) {
            lookupDependency(db, words, showTime, minOccurrences);
            return;
        }
        
        // Special handling for NER date index
        if (indexType.equals("ner_date")) {
            String dateStr = words.get(0);
            if (!dateStr.matches("\\d{8}")) {
                System.err.println("Error: Date must be in YYYYMMDD format");
                return;
            }
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
        if (positions.size() < minOccurrences) {
            System.out.printf("%s found but has fewer than %d occurrences (%d)%n",
                    formatSearchTerm(words, indexType), minOccurrences, positions.size());
            return;
        }

        System.out.printf("Found %s in %d positions:%n",
                formatSearchTerm(words, indexType), positions.size());

        // Sort positions by date if showing temporal distribution
        if (showTime) {
            List<Position> sortedPositions = new ArrayList<>(positions.getPositions());
            sortedPositions.sort((a, b) -> a.getTimestamp().compareTo(b.getTimestamp()));
            
            // Group positions by timestamp if showing temporal distribution
            Map<String, Integer> timeDistribution = new TreeMap<>();
            for (Position pos : sortedPositions) {
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
            case "ner_date" -> String.format("date '%s-%s-%s'",
                    words.get(0).substring(0, 4),
                    words.get(0).substring(4, 6),
                    words.get(0).substring(6, 8));
            case "pos" -> String.format("POS tag '%s'", words.get(0).toLowerCase());
            default -> throw new IllegalArgumentException("Invalid index type");
        };
    }

    private static void lookupDependency(DB db, List<String> pattern, boolean showTime, int minOccurrences) throws IOException {
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

        // Filter matches by minimum occurrences
        matches.entrySet().removeIf(entry -> entry.getValue().size() < minOccurrences);

        if (matches.isEmpty()) {
            System.out.printf("No matches found for dependency pattern %s with at least %d occurrences%n",
                    formatDependencyPattern(headToken, relation, depToken), minOccurrences);
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

    private static void listEntries(DB db, String indexType, boolean showCounts, int minOccurrences) throws IOException {
        System.out.printf("Listing entries in %s index:%n", indexType);
        int totalEntries = 0;
        
        try (DBIterator iterator = db.iterator()) {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                String key = asString(iterator.peekNext().getKey());
                PositionList positions = PositionList.deserialize(iterator.peekNext().getValue());
                
                if (positions.size() < minOccurrences) {
                    continue;
                }

                totalEntries++;
                if (indexType.equals("dependency")) {
                    System.out.print(formatDependencyKey(key));
                } else if (indexType.equals("ner_date")) {
                    // Format date as YYYY-MM-DD
                    System.out.printf("Date: %s-%s-%s",
                            key.substring(0, 4),
                            key.substring(4, 6),
                            key.substring(6, 8));
                } else {
                    System.out.print(key.replace(DELIMITER, " "));
                }

                if (showCounts) {
                    System.out.printf(" (%d occurrences)", positions.size());
                }
                System.out.println();
            }
        }
        
        System.out.printf("%nTotal entries: %d%n", totalEntries);
    }

    private static String formatDependencyKey(String key) {
        String[] parts = key.split(DELIMITER);
        if (parts.length != 3) return key;
        return String.format("%s-%s->%s", parts[0], parts[1], parts[2]);
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void showSummary(DB db, String indexType) throws IOException {
        logger.debug("Generating summary for {} index", indexType);
        
        long totalEntries = 0;
        long totalOccurrences = 0;
        long minOccurrences = Long.MAX_VALUE;
        long maxOccurrences = 0;
        Map<Integer, Long> occurrenceDistribution = new TreeMap<>();

        try (DBIterator iterator = db.iterator()) {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                totalEntries++;
                
                PositionList positions = PositionList.deserialize(iterator.peekNext().getValue());
                int occurrences = positions.size();
                totalOccurrences += occurrences;
                
                minOccurrences = Math.min(minOccurrences, occurrences);
                maxOccurrences = Math.max(maxOccurrences, occurrences);
                
                // Track distribution in buckets
                int bucket = getBucket(occurrences);
                occurrenceDistribution.merge(bucket, 1L, Long::sum);
            }
        }

        if (totalEntries == 0) {
            System.out.println("Index is empty");
            return;
        }

        System.out.println("\nIndex Summary:");
        System.out.println("=============");
        System.out.printf("Index type: %s%n", indexType);
        System.out.printf("Total unique entries: %,d%n", totalEntries);
        System.out.printf("Total occurrences: %,d%n", totalOccurrences);
        System.out.printf("Average occurrences per entry: %.2f%n", (double) totalOccurrences / totalEntries);
        System.out.printf("Min occurrences for an entry: %d%n", minOccurrences);
        System.out.printf("Max occurrences for an entry: %d%n", maxOccurrences);
        
        System.out.println("\nOccurrence Distribution:");
        System.out.println("======================");
        for (Map.Entry<Integer, Long> entry : occurrenceDistribution.entrySet()) {
            String range = formatBucketRange(entry.getKey());
            double percentage = (entry.getValue() * 100.0) / totalEntries;
            System.out.printf("%s occurrences: %,d entries (%.1f%%)%n", 
                    range, entry.getValue(), percentage);
        }
        System.out.println();
    }

    private static int getBucket(int occurrences) {
        if (occurrences == 1) return 1;
        if (occurrences <= 5) return 5;
        if (occurrences <= 10) return 10;
        if (occurrences <= 50) return 50;
        if (occurrences <= 100) return 100;
        if (occurrences <= 500) return 500;
        if (occurrences <= 1000) return 1000;
        return Integer.MAX_VALUE;
    }

    private static String formatBucketRange(int bucket) {
        return switch (bucket) {
            case 1 -> "1";
            case 5 -> "2-5";
            case 10 -> "6-10";
            case 50 -> "11-50";
            case 100 -> "51-100";
            case 500 -> "101-500";
            case 1000 -> "501-1000";
            default -> "1000+";
        };
    }
}
