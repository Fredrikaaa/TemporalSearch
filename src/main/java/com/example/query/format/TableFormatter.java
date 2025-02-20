package com.example.query.format;

import java.io.IOException;
import java.util.List;

/**
 * Interface for formatting query results into tables.
 */
public interface TableFormatter {
    /**
     * Formats a list of rows into a preview table for console display.
     * @param headers The column headers
     * @param rows The data rows
     * @param config The table configuration
     * @return A string containing the formatted preview table
     */
    String formatPreview(List<String> headers, List<List<TableCell>> rows, TableConfig config);

    /**
     * Writes the table data to a CSV file.
     * @param headers The column headers
     * @param rows The data rows
     * @param config The table configuration
     * @throws IOException if there is an error writing to the file
     */
    void writeCSV(List<String> headers, List<List<TableCell>> rows, TableConfig config) throws IOException;

    /**
     * Gets the list of column formats used by this formatter.
     * @return The list of column formats
     */
    List<ColumnFormat> getColumnFormats();
} 