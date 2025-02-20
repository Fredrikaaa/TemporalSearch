package com.example.query.format;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Base implementation of TableFormatter with common functionality.
 */
public class BaseTableFormatter implements TableFormatter {
    private static final Logger logger = Logger.getLogger(BaseTableFormatter.class.getName());
    private final List<ColumnFormat> columnFormats;

    /**
     * Creates a new base table formatter.
     * @param columnFormats The list of column formats to use
     */
    public BaseTableFormatter(List<ColumnFormat> columnFormats) {
        this.columnFormats = new ArrayList<>(columnFormats);
    }

    @Override
    public List<ColumnFormat> getColumnFormats() {
        return new ArrayList<>(columnFormats);
    }

    @Override
    public String formatPreview(List<String> headers, List<List<TableCell>> rows, TableConfig config) {
        if (rows.isEmpty()) {
            return "No results to display";
        }

        // Calculate column widths
        int[] widths = calculateColumnWidths(headers, rows, config.maxCellWidth());

        // Build the preview table
        StringBuilder preview = new StringBuilder();

        // Add headers if enabled
        if (config.showHeaders()) {
            preview.append(formatRow(headers, widths, true));
            preview.append(formatSeparator(widths));
        }

        // Add data rows (limited by previewRows)
        int rowCount = Math.min(rows.size(), config.previewRows());
        for (int i = 0; i < rowCount; i++) {
            List<String> displayValues = rows.get(i).stream()
                .map(TableCell::displayValue)
                .collect(Collectors.toList());
            preview.append(formatRow(displayValues, widths, false));
        }

        // Add summary if there are more rows
        if (rows.size() > config.previewRows()) {
            preview.append(String.format("%n(%d more rows, saved to %s)%n", 
                rows.size() - config.previewRows(), config.outputFile()));
        }

        return preview.toString();
    }

    @Override
    public void writeCSV(List<String> headers, List<List<TableCell>> rows, TableConfig config) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(config.outputFile()))) {
            // Write headers if enabled
            if (config.showHeaders()) {
                writer.write(toCSVLine(headers, config.delimiter()));
                writer.newLine();
            }

            // Write data rows
            for (List<TableCell> row : rows) {
                List<String> values = row.stream()
                    .map(TableCell::rawValue)
                    .collect(Collectors.toList());
                writer.write(toCSVLine(values, config.delimiter()));
                writer.newLine();
            }
        }
        logger.info("Wrote " + rows.size() + " rows to " + config.outputFile());
    }

    /**
     * Calculates the display width for each column.
     */
    private int[] calculateColumnWidths(List<String> headers, List<List<TableCell>> rows, int maxWidth) {
        int columnCount = columnFormats.size();
        int[] widths = new int[columnCount];

        // Initialize with header widths if showing headers
        for (int i = 0; i < columnCount; i++) {
            widths[i] = Math.min(headers.get(i).length(), maxWidth);
        }

        // Update with maximum content widths
        for (List<TableCell> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                widths[i] = Math.min(
                    Math.max(widths[i], row.get(i).width()),
                    maxWidth
                );
            }
        }

        return widths;
    }

    /**
     * Formats a single row of the preview table.
     */
    private String formatRow(List<String> values, int[] widths, boolean isHeader) {
        StringBuilder row = new StringBuilder();
        row.append("| ");

        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            ColumnFormat format = columnFormats.get(i);
            
            // Truncate and pad the value according to alignment
            String formatted = formatCell(value, widths[i], format.align());
            row.append(formatted).append(" | ");
        }

        row.append("\n");
        return row.toString();
    }

    /**
     * Formats a separator line for the preview table.
     */
    private String formatSeparator(int[] widths) {
        return "+" + IntStream.of(widths)
            .mapToObj(w -> "-".repeat(w + 2))
            .collect(Collectors.joining("+")) + "+\n";
    }

    /**
     * Formats a cell value with proper alignment and width.
     */
    private String formatCell(String value, int width, ColumnFormat.TextAlign align) {
        if (value.length() > width) {
            return value.substring(0, width - 3) + "...";
        }

        int padding = width - value.length();
        return switch (align) {
            case LEFT -> value + " ".repeat(padding);
            case RIGHT -> " ".repeat(padding) + value;
            case CENTER -> {
                int leftPad = padding / 2;
                int rightPad = padding - leftPad;
                yield " ".repeat(leftPad) + value + " ".repeat(rightPad);
            }
        };
    }

    /**
     * Converts a list of values to a CSV line.
     */
    private String toCSVLine(List<String> values, char delimiter) {
        return values.stream()
            .map(this::escapeCSV)
            .collect(Collectors.joining(String.valueOf(delimiter)));
    }

    /**
     * Escapes a value for CSV output.
     */
    private String escapeCSV(String value) {
        if (value == null) {
            return "";
        }

        // Quote the value if it contains special characters
        if (value.contains("\"") || value.contains(",") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
} 