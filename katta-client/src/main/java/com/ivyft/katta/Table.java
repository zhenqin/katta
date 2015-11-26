package com.ivyft.katta;

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-3-22
 * Time: 下午1:42
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class Table {

    private String[] header;
    private final List<String[]> rows = new ArrayList<String[]>();
    private boolean batchMode;
    private boolean skipColumnNames;

    public Table(final String... header) {
        this.header = header;
    }

    /**
     * Set the header later by calling setHeader()
     */
    public Table() {
        // default constructor
    }

    public void setHeader(String... header) {
        this.header = header;
    }

    public boolean isBatchMode() {
        return batchMode;
    }

    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
    }

    public boolean isSkipColumnNames() {
        return skipColumnNames;
    }

    public void setSkipColumnNames(boolean skipCoulmnNames) {
        this.skipColumnNames = skipCoulmnNames;
    }

    public void addRow(final Object... row) {
        String[] strs = new String[row.length];
        for (int i = 0; i < row.length; i++) {
            strs[i] = row[i] != null ? row[i].toString() : "";
        }
        rows.add(strs);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        final int[] columnSizes = getColumnSizes();
        int rowWidth = 0;
        for (final int columnSize : columnSizes) {
            rowWidth += columnSize;
        }
        rowWidth += 2 + (Math.max(0, columnSizes.length - 1) * 3) + 2;
        String leftPad = "";
        if (!batchMode) {
            builder.append('\n').append(getChar(rowWidth, "-")).append('\n');
        }
        if (!skipColumnNames) {
            // Header.
            if (!batchMode) {
                builder.append("| ");
            }
            for (int i = 0; i < header.length; i++) {
                final String column = header[i];
                builder.append(leftPad);
                builder.append(column).append(getChar(columnSizes[i] - column.length(), " "));
                if (!batchMode) {
                    leftPad = " | ";
                } else {
                    leftPad = " ";
                }
            }
            if (!batchMode) {
                builder.append(" |\n").append(getChar(rowWidth, "="));// .append('\n');
            }
            builder.append('\n');
        }
        // Rows.
        for (final Object[] row : rows) {
            if (!batchMode) {
                builder.append("| ");
            }
            leftPad = "";
            for (int i = 0; i < row.length; i++) {
                builder.append(leftPad);
                builder.append(row[i]);
                builder.append(getChar(columnSizes[i] - row[i].toString().length(), " "));
                if (!batchMode) {
                    leftPad = " | ";
                } else {
                    leftPad = " ";
                }
            }
            if (!batchMode) {
                builder.append(" |\n").append(getChar(rowWidth, "-"));
            }
            builder.append('\n');
        }

        return builder.toString();
    }

    private String getChar(final int count, final String character) {
        String spaces = "";
        for (int j = 0; j < count; j++) {
            spaces += character;
        }
        return spaces;
    }

    private int[] getColumnSizes() {
        final int[] sizes = new int[header.length];
        for (int i = 0; i < sizes.length; i++) {
            int min = header[i].length();
            for (final String[] row : rows) {
                int rowLength = row[i].length();
                if (rowLength > min) {
                    min = rowLength;
                }
            }
            sizes[i] = min;
        }

        return sizes;
    }
}
