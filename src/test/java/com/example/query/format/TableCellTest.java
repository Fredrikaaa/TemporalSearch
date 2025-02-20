package com.example.query.format;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

class TableCellTest {

    @Test
    void testValidTableCell() {
        TableCell cell = new TableCell("raw", "display", Arrays.asList("*raw*"), 4);
        assertEquals("raw", cell.rawValue());
        assertEquals("display", cell.displayValue());
        assertEquals(Arrays.asList("*raw*"), cell.highlights());
        assertEquals(4, cell.width());
    }

    @Test
    void testSimpleFactoryMethod() {
        TableCell cell = TableCell.of("test");
        assertEquals("test", cell.rawValue());
        assertEquals("test", cell.displayValue());
        assertTrue(cell.highlights().isEmpty());
        assertEquals(4, cell.width());
    }

    @Test
    void testHighlightFactoryMethod() {
        List<String> highlights = Arrays.asList("*test*", "[test]");
        TableCell cell = TableCell.withHighlights("test", highlights);
        assertEquals("test", cell.rawValue());
        assertEquals("test", cell.displayValue());
        assertEquals(highlights, cell.highlights());
        assertEquals(4, cell.width());
    }

    @Test
    void testNullValueInSimpleFactory() {
        TableCell cell = TableCell.of(null);
        assertEquals("", cell.rawValue());
        assertEquals("", cell.displayValue());
        assertTrue(cell.highlights().isEmpty());
        assertEquals(0, cell.width());
    }

    @Test
    void testNullValueInHighlightFactory() {
        List<String> highlights = Arrays.asList("*test*");
        TableCell cell = TableCell.withHighlights(null, highlights);
        assertEquals("", cell.rawValue());
        assertEquals("", cell.displayValue());
        assertEquals(highlights, cell.highlights());
        assertEquals(0, cell.width());
    }

    @Test
    void testNullRawValueThrowsException() {
        assertThrows(NullPointerException.class, () ->
            new TableCell(null, "display", Arrays.asList("*raw*"), 4));
    }

    @Test
    void testNullDisplayValueThrowsException() {
        assertThrows(NullPointerException.class, () ->
            new TableCell("raw", null, Arrays.asList("*raw*"), 4));
    }

    @Test
    void testNegativeWidthThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
            new TableCell("raw", "display", Arrays.asList("*raw*"), -1));
    }

    @Test
    void testNullHighlightsCreatesEmptyList() {
        TableCell cell = new TableCell("raw", "display", null, 4);
        assertNotNull(cell.highlights());
        assertTrue(cell.highlights().isEmpty());
    }

    @Test
    void testHighlightsDefensiveCopy() {
        List<String> highlights = new ArrayList<>();
        highlights.add("*test*");
        TableCell cell = new TableCell("raw", "display", highlights, 4);
        
        // Verify that modifying the original list doesn't affect the cell
        highlights.clear();
        assertEquals(List.of("*test*"), cell.highlights());
        
        // Verify that modifying the returned list doesn't affect the cell
        List<String> returnedHighlights = new ArrayList<>(cell.highlights());
        returnedHighlights.clear();
        assertEquals(List.of("*test*"), cell.highlights());
    }
} 