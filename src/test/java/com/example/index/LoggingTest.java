package com.example.index;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the logging functionality of the PositionList class.
 */
public class LoggingTest {
    private ListAppender<ILoggingEvent> listAppender;
    private Logger logger;

    @BeforeEach
    void setUp() {
        // Get Logback Logger
        logger = (Logger) LoggerFactory.getLogger(PositionList.class);
        
        // Create and start a ListAppender
        listAppender = new ListAppender<>();
        listAppender.start();
        
        // Add the appender to the logger
        logger.addAppender(listAppender);
        
        // Set the level to DEBUG to capture all logs
        logger.setLevel(Level.DEBUG);
    }

    @Test
    void testNullPositionLogging() {
        PositionList list = new PositionList();
        list.add(null);

        // Verify warning was logged
        List<ILoggingEvent> logsList = listAppender.list;
        assertFalse(logsList.isEmpty());
        assertEquals(Level.WARN, logsList.get(0).getLevel());
        assertEquals("Attempted to add null position", logsList.get(0).getMessage());
    }

    @Test
    void testMergeLogging() {
        PositionList list1 = new PositionList();
        PositionList list2 = new PositionList();

        // Add some test positions
        Position pos1 = new Position(1, 1, 0, 5, LocalDate.now());
        Position pos2 = new Position(1, 1, 6, 10, LocalDate.now());
        Position pos3 = new Position(1, 1, 0, 5, LocalDate.now()); // Duplicate of pos1

        list1.add(pos1);
        list2.add(pos2);
        list2.add(pos3);

        // Clear previous logs
        listAppender.list.clear();

        // Perform merge
        list1.merge(list2);

        // Verify merge logging
        List<ILoggingEvent> logsList = listAppender.list;
        assertFalse(logsList.isEmpty());
        
        // Find the merge log message
        boolean foundMergeLog = false;
        for (ILoggingEvent event : logsList) {
            if (event.getMessage().contains("Merged position lists")) {
                foundMergeLog = true;
                assertEquals(Level.DEBUG, event.getLevel());
                // Verify the log contains the correct counts
                String message = event.getFormattedMessage();
                assertTrue(message.contains("initial: 1"));
                assertTrue(message.contains("other: 2"));
                assertTrue(message.contains("final: 2"));
                assertTrue(message.contains("duplicates: 1"));
                break;
            }
        }
        assertTrue(foundMergeLog, "Merge log message not found");
    }

    @Test
    void testSerializationLogging() {
        PositionList list = new PositionList();
        Position pos = new Position(1, 1, 0, 5, LocalDate.now());
        list.add(pos);

        // Clear previous logs
        listAppender.list.clear();

        // Perform serialization
        byte[] serialized = list.serialize();
        PositionList.deserialize(serialized);

        // Verify serialization logging
        List<ILoggingEvent> logsList = listAppender.list;
        assertFalse(logsList.isEmpty());

        // Verify deserialization logs exist (these are not sampled)
        boolean foundDeserializeLog = false;
        for (ILoggingEvent event : logsList) {
            String message = event.getFormattedMessage();
            if (message.contains("Deserializing")) {
                foundDeserializeLog = true;
                break;
            }
        }
        assertTrue(foundDeserializeLog, "Deserialization log not found");

        // Note: Serialization logs are sampled, so we don't verify them in the test
        // as they may not appear every time
    }
}