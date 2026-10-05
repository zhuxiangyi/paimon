/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Encoding of map-delta columns in the write columns of a data evolution file.
 *
 * <p>With {@code data-evolution.map-delta.enabled}, an update of a top-level {@code MAP} column
 * {@code m} may write a delta file that stores, per row, only the entries to merge into the current
 * value instead of the whole map. Such a file lists the column in its write columns like any other,
 * in physical order, and appends a marker {@code _MAP_DELTA_m} that is not a physical column, for
 * example {@code [m, d, _MAP_DELTA_m]}. A reader merges the delta values of a row, oldest first,
 * into the value of the latest file that stores the column whole, exactly like {@code
 * map_concat(base, delta)} with the last value winning for a duplicated key:
 *
 * <ul>
 *   <li>a {@code NULL} base or a {@code NULL} delta yields {@code NULL};
 *   <li>an empty delta leaves the value unchanged;
 *   <li>otherwise the delta entries are put into the base, a new key is appended and an existing
 *       key keeps its position.
 * </ul>
 *
 * <p>An entry is a marker only if it does not name a field itself, starts with {@link #PREFIX}, the
 * rest names a top-level {@code MAP} field, and that field is also one of the write columns. Files
 * written without the feature only contain field names, which therefore keep their meaning.
 *
 * <p>Keeping the column itself in the write columns matters for readers that predate map deltas:
 * they keep the file for a read of the column, and fail on the marker, which is not a field of
 * theirs, instead of skipping the file and returning the value before the deltas.
 */
public final class MapDeltaColumns {

    public static final String PREFIX = "_MAP_DELTA_";

    private MapDeltaColumns() {}

    /**
     * Whether a map with keys of this type can be written as map deltas. Deltas are merged by value
     * equality of the stored keys, which is the equality of {@code map_concat} only if engines
     * compare keys of this type exactly as they are stored: engines normalize {@code -0.0} and
     * {@code NaN} of floating point keys, compare timestamps, decimals and times at their own
     * precision before the value is converted to the column type, and pad or trim fixed-length
     * types, and keys of nested types have no value equality in every representation.
     */
    public static boolean supportsKeyType(DataType keyType) {
        return keyType.isAnyOf(
                DataTypeRoot.VARCHAR,
                DataTypeRoot.VARBINARY,
                DataTypeRoot.BOOLEAN,
                DataTypeRoot.TINYINT,
                DataTypeRoot.SMALLINT,
                DataTypeRoot.INTEGER,
                DataTypeRoot.BIGINT,
                DataTypeRoot.DATE);
    }

    /** Whether a column of this type can be written as map deltas. */
    public static boolean supportsType(DataType type) {
        return type instanceof MapType && supportsKeyType(((MapType) type).getKeyType());
    }

    /**
     * Whether a write of {@code writeType} goes through the writer of dedicated blob or vector
     * files, which does not record map deltas: the table stores a blob field in blob files and the
     * write contains a blob field, or the write contains a field stored in vector files.
     */
    public static boolean writesDedicatedFiles(
            RowType tableType, RowType writeType, CoreOptions options) {
        boolean tableHasBlobFiles =
                !BlobType.fieldsInBlobFile(tableType, options.blobInlineField()).isEmpty();
        boolean writesBlob = writeType.getFieldTypes().stream().anyMatch(BlobType::isBlobFileField);
        return (tableHasBlobFiles && writesBlob)
                || !VectorType.fieldNamesInVectorFile(writeType, options.withVectorFormat())
                        .isEmpty();
    }

    /**
     * Checks that a write of {@code writeType} to a table of {@code tableType} can store {@code
     * columns} as map deltas. Throws {@link IllegalArgumentException} for a column that is not a
     * top-level map column of the write, and {@link UnsupportedOperationException} for a write the
     * table or the column cannot store as map deltas.
     */
    public static void validate(
            RowType tableType, RowType writeType, Collection<String> columns, CoreOptions options) {
        if (columns.isEmpty()) {
            return;
        }
        if (!options.dataEvolutionEnabled() || !options.dataEvolutionMapDeltaEnabled()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Writing map deltas of %s requires %s=true and %s=true.",
                            columns,
                            CoreOptions.DATA_EVOLUTION_ENABLED.key(),
                            CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key()));
        }
        for (String column : columns) {
            if (!writeType.containsField(column) || !tableType.containsField(column)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Map delta column '%s' is not a column updated as a whole by the "
                                        + "write of %s.",
                                column, writeType.getFieldNames()));
            }
            DataType type = writeType.getField(column).type();
            if (!(type instanceof MapType)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Map delta column '%s' is not a map column but %s.", column, type));
            }
            if (!supportsType(type)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Map delta column '%s' has keys of type %s, which cannot be "
                                        + "merged as map deltas.",
                                column, ((MapType) type).getKeyType()));
            }
            if (tableType.containsField(encode(column))) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Cannot write a map delta of column '%s' because the table has a "
                                        + "column named '%s', which is how the delta is recorded.",
                                column, encode(column)));
            }
        }
        if (writesDedicatedFiles(tableType, writeType, options)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Map deltas cannot be written together with columns stored in "
                                    + "dedicated blob or vector files: %s.",
                            writeType.getFieldNames()));
        }
    }

    /** The marker appended to the write columns of a file storing {@code column} as map deltas. */
    public static String encode(String column) {
        return PREFIX + column;
    }

    /**
     * Returns the name of the {@code MAP} column whose marker {@code writeCol} is, or {@code null}
     * if {@code writeCol} is not a marker name of {@code rowType}. This only looks at the name: a
     * marker only counts if the write columns also list its column, see {@link
     * #deltaColumns(RowType, List)}.
     */
    @Nullable
    public static String decode(RowType rowType, String writeCol) {
        if (!writeCol.startsWith(PREFIX) || rowType.containsField(writeCol)) {
            return null;
        }
        String column = writeCol.substring(PREFIX.length());
        if (!rowType.containsField(column)) {
            return null;
        }
        return rowType.getField(column).type() instanceof MapType ? column : null;
    }

    /**
     * The physical columns of {@code writeCols}, without the markers of map deltas. Returns {@code
     * writeCols} itself if it has none.
     */
    public static List<String> toPhysical(RowType rowType, List<String> writeCols) {
        Set<String> markers = markers(rowType, writeCols);
        if (markers.isEmpty()) {
            return writeCols;
        }
        List<String> physical = new ArrayList<>(writeCols.size() - markers.size());
        for (String writeCol : writeCols) {
            if (!markers.contains(writeCol)) {
                physical.add(writeCol);
            }
        }
        return physical;
    }

    /**
     * Like {@link #toPhysical(RowType, List)} for the fields of a schema, only building a row type
     * if a write column may record a map delta.
     */
    public static List<String> toPhysical(List<DataField> fields, List<String> writeCols) {
        return mayContainDeltas(writeCols) ? toPhysical(new RowType(fields), writeCols) : writeCols;
    }

    /** Whether some of {@code writeCols} may record a map delta, without resolving them. */
    public static boolean mayContainDeltas(@Nullable List<String> writeCols) {
        if (writeCols == null) {
            return false;
        }
        for (String writeCol : writeCols) {
            if (writeCol.startsWith(PREFIX)) {
                return true;
            }
        }
        return false;
    }

    /** Whether a file written with {@code schema} and {@code writeCols} holds map deltas. */
    public static boolean hasDeltas(TableSchema schema, @Nullable List<String> writeCols) {
        return !deltaFieldIds(schema, writeCols).isEmpty();
    }

    /**
     * The ids of the {@code MAP} fields that {@code writeCols} of a file written with {@code
     * schema} records as deltas. Cheap for files without map deltas.
     */
    public static Set<Integer> deltaFieldIds(TableSchema schema, @Nullable List<String> writeCols) {
        return mayContainDeltas(writeCols)
                ? deltaFieldIds(schema.logicalRowType(), writeCols)
                : Collections.emptySet();
    }

    /** The names of the {@code MAP} columns that {@code writeCols} records as deltas. */
    public static Set<String> deltaColumns(RowType rowType, @Nullable List<String> writeCols) {
        if (writeCols == null) {
            return Collections.emptySet();
        }
        Set<String> columns = null;
        for (String marker : markers(rowType, writeCols)) {
            if (columns == null) {
                columns = new LinkedHashSet<>();
            }
            columns.add(decode(rowType, marker));
        }
        return columns == null ? Collections.emptySet() : columns;
    }

    /** The markers in {@code writeCols}: marker names whose column {@code writeCols} also lists. */
    private static Set<String> markers(RowType rowType, List<String> writeCols) {
        Set<String> markers = null;
        for (String writeCol : writeCols) {
            String column = decode(rowType, writeCol);
            if (column != null && writeCols.contains(column)) {
                if (markers == null) {
                    markers = new LinkedHashSet<>();
                }
                markers.add(writeCol);
            }
        }
        return markers == null ? Collections.emptySet() : markers;
    }

    /** The ids of the {@code MAP} fields that {@code writeCols} records as deltas. */
    public static Set<Integer> deltaFieldIds(RowType rowType, @Nullable List<String> writeCols) {
        Set<String> columns = deltaColumns(rowType, writeCols);
        if (columns.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Integer> ids = new LinkedHashSet<>();
        for (DataField field : rowType.getFields()) {
            if (columns.contains(field.name())) {
                ids.add(field.id());
            }
        }
        return ids;
    }
}
