package com.example;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;
import com.example.core.Position;
import com.example.core.PositionList;

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
                .choices("unigram", "bigram", "trigram", "dependency", "ner_date", "pos", "hypernym")
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

        parser.addArgument("--top")
                .type(Integer.class)
                .setDefault(0)
                .help("Show only top N categories (for hypernym index)");

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
            int topN = ns.getInt("top");

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
                    if (indexType.equals("dependency")) {
                        lookupDependency(db, words, showTime, minOccurrences);
                    } else if (indexType.equals("hypernym")) {
                        lookupHypernym(db, words, showTime, minOccurrences);
                    } else {
                        lookupWords(db, words, indexType, showTime, minOccurrences);
                    }
                } else if (listEntries || showCounts) {
                    listEntries(db, indexType, showCounts, minOccurrences, topN);
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
            case "bigram", "hypernym" -> 2;
            case "trigram" -> 3;
            case "dependency" -> 3;
            default -> throw new IllegalArgumentException("Unknown index type: " + indexType);
        };
    }

    private static void lookupWords(DB db, List<String> words, String indexType, boolean showTime, int minOccurrences) throws IOException {
        // Create key based on index type
        String key = switch (indexType) {
            case "unigram" -> words.get(0).toLowerCase();
            case "bigram" -> String.join(DELIMITER, words.stream().map(String::toLowerCase)
                    .toList());
            case "trigram" -> String.join(DELIMITER, words.stream().map(String::toLowerCase)
                    .toList());
            case "ner_date" -> words.get(0);
            case "pos" -> words.get(0).toLowerCase();
            default -> throw new IllegalArgumentException("Invalid index type: " + indexType);
        };

        // Get positions for key
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
            case "hypernym" -> String.format("hypernym relation '%s' -> '%s'", 
                    words.get(0).toLowerCase(), words.get(1).toLowerCase());
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
            if (pattern.size() == 1) {
                // Search for head token only
                iterator.seek(bytes(headToken + DELIMITER));
            } else {
                // Search for specific pattern
                iterator.seek(bytes(searchPattern));
            }
            
            while (iterator.hasNext()) {
                String key = asString(iterator.peekNext().getKey());
                logger.debug("Examining key: {}", key);
                
                String[] parts = key.split(DELIMITER);
                
                if (parts.length != 3) continue;
                
                // Stop if we've moved past potential matches for the head token
                if (!parts[0].equals(headToken) && pattern.size() == 1) {
                    break;
                }
                
                // For full pattern matching
                if (pattern.size() > 1 && (!parts[0].equals(headToken) || !parts[1].equals(relation))) {
                    if (parts[0].compareTo(headToken) > 0) break;
                    iterator.next();
                    continue;
                }
                
                // For three-part pattern
                if (pattern.size() > 2 && !parts[2].equals(depToken)) {
                    iterator.next();
                    continue;
                }
                
                matches.put(key, PositionList.deserialize(iterator.peekNext().getValue()));
                iterator.next();
            }
        }

        if (matches.isEmpty()) {
            if (pattern.size() == 1) {
                System.out.printf("No dependencies found with head token '%s'%n", headToken);
            } else {
                System.out.printf("No matches found for dependency pattern: %s%n",
                        formatDependencyPattern(headToken, relation, depToken));
            }
            return;
        }

        // Filter matches by minimum occurrences
        matches.entrySet().removeIf(entry -> entry.getValue().size() < minOccurrences);

        if (matches.isEmpty()) {
            System.out.printf("No matches found with at least %d occurrences%n", minOccurrences);
            return;
        }

        // Sort matches by relation and dependent token
        List<Map.Entry<String, PositionList>> sortedMatches = new ArrayList<>(matches.entrySet());
        sortedMatches.sort((a, b) -> {
            String[] partsA = a.getKey().split(DELIMITER);
            String[] partsB = b.getKey().split(DELIMITER);
            int relCompare = partsA[1].compareTo(partsB[1]);
            return relCompare != 0 ? relCompare : partsA[2].compareTo(partsB[2]);
        });

        if (pattern.size() == 1) {
            System.out.printf("Found dependencies for head token '%s':%n", headToken);
        } else {
            System.out.printf("Found matches for dependency pattern '%s-%s->%s':%n",
                    headToken, relation, depToken);
        }

        for (Map.Entry<String, PositionList> entry : sortedMatches) {
            String[] parts = entry.getKey().split(DELIMITER);
            PositionList positions = entry.getValue();

            if (showTime) {
                showTemporalDistribution(parts, positions);
            } else {
                showPositions(parts, positions);
            }
        }
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

    private static void listEntries(DB db, String indexType, boolean showCounts, int minOccurrences, int topN) throws IOException {
        System.out.printf("Listing entries in %s index:%n", indexType);
        int totalEntries = 0;
        
        if (indexType.equals("hypernym")) {
            // Use TreeMap for sorted categories
            Map<String, Map<String, Integer>> hypernymGroups = new TreeMap<>();
            Map<String, Integer> categoryTotalOccurrences = new HashMap<>();
            
            try (DBIterator iterator = db.iterator()) {
                iterator.seekToFirst();
                
                while (iterator.hasNext()) {
                    String key = asString(iterator.peekNext().getKey());
                    PositionList positions = PositionList.deserialize(iterator.peekNext().getValue());
                    
                    if (positions.size() >= minOccurrences) {
                        String[] parts = key.split(DELIMITER);
                        if (parts.length == 2) {
                            String category = parts[0];
                            String instance = parts[1];
                            hypernymGroups.computeIfAbsent(category, k -> new TreeMap<>())
                                        .put(instance, positions.size());
                            categoryTotalOccurrences.merge(category, positions.size(), Integer::sum);
                        }
                    }
                    iterator.next();
                }
            }

            // Sort categories by total occurrences if showing counts, otherwise by number of instances
            List<Map.Entry<String, Map<String, Integer>>> sortedCategories = new ArrayList<>(hypernymGroups.entrySet());
            if (showCounts) {
                sortedCategories.sort((a, b) -> categoryTotalOccurrences.get(b.getKey()).compareTo(categoryTotalOccurrences.get(a.getKey())));
            } else {
                sortedCategories.sort((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()));
            }

            // Get top N if specified
            if (topN > 0) {
                sortedCategories = sortedCategories.subList(0, Math.min(topN, sortedCategories.size()));
            }
            
            // Print grouped results
            for (Map.Entry<String, Map<String, Integer>> categoryEntry : sortedCategories) {
                String category = categoryEntry.getKey();
                Map<String, Integer> instances = categoryEntry.getValue();
                int totalOccurrences = categoryTotalOccurrences.get(category);
                
                if (showCounts) {
                    System.out.printf("  %s (%d total occurrences) -> %s%n", category, totalOccurrences,
                            instances.entrySet().stream()
                                    .map(e -> String.format("%s (%d)", e.getKey(), e.getValue()))
                                    .collect(Collectors.joining(", ")));
                } else {
                    System.out.printf("  %s (%d instances) -> %s%n", category, instances.size(),
                            String.join(", ", instances.keySet()));
                }
                totalEntries++;
            }
        } else {
            try (DBIterator iterator = db.iterator()) {
                iterator.seekToFirst();
                
                while (iterator.hasNext()) {
                    String key = asString(iterator.peekNext().getKey());
                    PositionList positions = PositionList.deserialize(iterator.peekNext().getValue());
                    
                    if (positions.size() < minOccurrences) {
                        iterator.next();
                        continue;
                    }
                    
                    if (showCounts) {
                        if (indexType.equals("dependency")) {
                            System.out.printf("  %s (%d occurrences)%n", formatDependencyKey(key), positions.size());
                        } else {
                            System.out.printf("  %s (%d occurrences)%n", key, positions.size());
                        }
                    } else {
                        if (indexType.equals("dependency")) {
                            System.out.printf("  %s%n", formatDependencyKey(key));
                        } else {
                            System.out.printf("  %s%n", key);
                        }
                    }
                    
                    totalEntries++;
                    iterator.next();
                }
            }
        }
        
        System.out.printf("%nTotal entries: %d%n", totalEntries);
    }

    private static String formatDependencyKey(String key) {
        String[] parts = key.split(DELIMITER);
        if (parts.length != 3) return key;
        return String.format("%s-%s->%s", parts[0], parts[1], parts[2]);
    }

    private static String formatHypernymKey(String key) {
        String[] parts = key.split(DELIMITER);
        if (parts.length != 2) return key;
        return String.format("%s -> %s", parts[0], parts[1]);
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

    private static void lookupHypernym(DB db, List<String> words, boolean showTime, int minOccurrences) throws IOException {
        if (words.size() != 2) {
            System.err.println("Error: hypernym index requires exactly 2 words (category instance)");
            System.exit(1);
        }

        String category = words.get(0).toLowerCase();
        String instance = words.get(1).toLowerCase();
        String key = category + DELIMITER + instance;

        // Get positions for key
        byte[] data = db.get(bytes(key));
        if (data == null) {
            System.out.printf("No hypernym relation found between category '%s' and instance '%s'%n",
                    category, instance);
            return;
        }

        PositionList positions = PositionList.deserialize(data);
        if (positions.size() < minOccurrences) {
            System.out.printf("Hypernym relation between '%s' and '%s' found but has fewer than %d occurrences (%d)%n",
                    category, instance, minOccurrences, positions.size());
            return;
        }

        System.out.printf("Found hypernym relation: '%s' is a category for '%s' in %d positions:%n",
                category, instance, positions.size());

        if (showTime) {
            List<Position> sortedPositions = new ArrayList<>(positions.getPositions());
            sortedPositions.sort((a, b) -> a.getTimestamp().compareTo(b.getTimestamp()));
            
            // Group positions by timestamp
            Map<String, Integer> timeDistribution = new TreeMap<>();
            for (Position pos : sortedPositions) {
                String yearMonth = pos.getTimestamp().toString().substring(0, 7); // YYYY-MM
                timeDistribution.merge(yearMonth, 1, Integer::sum);
            }

            System.out.println("\nTemporal distribution:");
            timeDistribution.forEach((date, count) -> System.out.printf("%s: %d occurrences%n", date, count));
        } else {
            for (Position pos : positions.getPositions()) {
                System.out.printf("  Document %d, Sentence %d, Chars %d-%d, Date: %s%n",
                        pos.getDocumentId(), pos.getSentenceId(),
                        pos.getBeginPosition(), pos.getEndPosition(),
                        pos.getTimestamp());
            }
        }
    }
}
