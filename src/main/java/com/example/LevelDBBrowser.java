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

public class LevelDBBrowser {
    private static final String DELIMITER = "\u0000";

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("LevelDBBrowser").build()
                .defaultHelp(true)
                .description("Browse contents of LevelDB index databases");

        parser.addArgument("index_type")
                .choices("unigram", "bigram", "trigram")
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

            // Construct full path to specific index
            String dbPath = basePath + "/" + indexType;

            // Validate word count matches index type
            if (words != null) {
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
            default -> throw new IllegalArgumentException("Invalid index type");
        };
    }

    private static void lookupWords(DB db, List<String> words, String indexType, boolean showTime) throws IOException {
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

    private static void listEntries(DB db, String indexType, boolean showCounts) throws IOException {
        Map<String, Integer> entries = new TreeMap<>();
        try (DBIterator iterator = db.iterator()) {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                String key = new String(iterator.peekNext().getKey(), "UTF-8");
                PositionList positions = PositionList.deserialize(iterator.peekNext().getValue());

                // Format key based on index type
                String displayKey = key.replace(DELIMITER, " ");
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

    private static String capitalizeFirst(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
