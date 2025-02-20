package com.example.query.format;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TableConfigTest {

    @Test
    void testValidConfiguration() {
        TableConfig config = new TableConfig(10, "test.csv", true, 50, ',');
        assertEquals(10, config.previewRows());
        assertEquals("test.csv", config.outputFile());
        assertTrue(config.showHeaders());
        assertEquals(50, config.maxCellWidth());
        assertEquals(',', config.delimiter());
    }

    @Test
    void testDefaultConfiguration() {
        TableConfig config = TableConfig.getDefault();
        assertEquals(10, config.previewRows());
        assertEquals("output.csv", config.outputFile());
        assertTrue(config.showHeaders());
        assertEquals(50, config.maxCellWidth());
        assertEquals(',', config.delimiter());
    }

    @Test
    void testNegativePreviewRowsThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
            new TableConfig(-1, "test.csv", true, 50, ','));
    }

    @Test
    void testInvalidMaxCellWidthThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
            new TableConfig(10, "test.csv", true, 0, ','));
    }

    @Test
    void testNullOutputFileThrowsException() {
        assertThrows(NullPointerException.class, () ->
            new TableConfig(10, null, true, 50, ','));
    }
} 