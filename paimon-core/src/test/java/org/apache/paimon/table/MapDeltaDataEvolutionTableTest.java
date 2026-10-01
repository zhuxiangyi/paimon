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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.FilesTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for <b>key-level</b> data evolution of map columns: an update that merges entries into a
 * map column writes a map-delta file holding only the entries to merge, aligned by row id, and a
 * read merges the deltas into the latest whole value like {@code map_concat(base, delta)}.
 */
public class MapDeltaDataEvolutionTableTest extends DataEvolutionTestBase {

    // id(0) INT, c(1) STRING, m(2) MAP<STRING, INT>, d(3) INT
    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("c", DataTypes.STRING())
                .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                .column("d", DataTypes.INT())
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testDeltasAreMergedOnRead() throws Exception {
        createTableDefault();
        writeBase(
                Arrays.asList(map("a", 1, "b", 2), map("a", 1), map("a", 1, "b", 2), map("x", 9)));

        // row 0 overrides a and adds c, row 1 is unchanged, row 2 adds c, row 3 adds y
        writeDelta(Arrays.asList(map("a", 10, "c", 3), map(), map("c", 3), map("y", 8)));
        // row 0 overrides c again, row 2 adds a key that the first delta did not know
        writeDelta(Arrays.asList(map("c", 30), map(), map("d", 4), map()));

        List<Map<String, Integer>> maps = readMaps();
        assertThat(maps).hasSize(4);
        // an existing key keeps its position, a new key is appended, the latest value wins
        assertThat(new ArrayList<>(maps.get(0).entrySet()))
                .containsExactlyElementsOf(map("a", 10, "b", 2, "c", 30).entrySet());
        assertThat(maps.get(1)).containsExactly(entry("a", 1));
        assertThat(new ArrayList<>(maps.get(2).entrySet()))
                .containsExactlyElementsOf(map("a", 1, "b", 2, "c", 3, "d", 4).entrySet());
        assertThat(new ArrayList<>(maps.get(3).entrySet()))
                .containsExactlyElementsOf(map("x", 9, "y", 8).entrySet());

        // the other columns still come from the base file
        List<InternalRow> rows = readRows(getTableDefault().rowType());
        for (int i = 0; i < rows.size(); i++) {
            assertThat(rows.get(i).getInt(0)).isEqualTo(i);
            assertThat(rows.get(i).getString(1).toString()).isEqualTo("c" + i);
            assertThat(rows.get(i).getInt(3)).isEqualTo(i * 10);
        }
    }

    @Test
    public void testNullSemanticsFollowMapConcat() throws Exception {
        createTableDefault();
        writeBase(Arrays.asList(null, map("a", 1), map("a", 1), map("a", 1), null));

        // map_concat(NULL, x) = NULL, map_concat(m, NULL) = NULL, an empty delta keeps the value,
        // and a NULL map value is a value like any other
        writeDelta(Arrays.asList(map("a", 2), null, map(), mapWithNullValue("a"), map()));

        List<Map<String, Integer>> maps = readMaps();
        assertThat(maps.get(0)).isNull();
        assertThat(maps.get(1)).isNull();
        assertThat(maps.get(2)).containsExactly(entry("a", 1));
        assertThat(maps.get(3)).containsExactly(entry("a", null));
        assertThat(maps.get(4)).isNull();

        // a later delta cannot bring a NULL map back
        writeDelta(Arrays.asList(map("b", 1), map("b", 1), map("b", 1), map("b", 1), map()));
        maps = readMaps();
        assertThat(maps.get(0)).isNull();
        assertThat(maps.get(1)).isNull();
        assertThat(maps.get(2)).containsExactly(entry("a", 1), entry("b", 1));
        assertThat(maps.get(3)).containsExactly(entry("a", null), entry("b", 1));
        assertThat(maps.get(4)).isNull();
    }

    @Test
    public void testWholeColumnWriteStopsTheMerge() throws Exception {
        createTableDefault();
        writeBase(Arrays.asList(map("a", 1), map("a", 1)));
        writeDelta(Arrays.asList(map("b", 2), map("b", 2)));

        // an ordinary column write replaces the value, older deltas no longer apply
        writeColumns(
                Collections.singletonList("m"),
                Arrays.asList(GenericRow.of(map("z", 0)), GenericRow.of((Object) null)));
        assertThat(readMaps()).containsExactly(map("z", 0), null);

        writeDelta(Arrays.asList(map("y", 1), map("y", 1)));
        List<Map<String, Integer>> maps = readMaps();
        assertThat(maps.get(0)).containsExactly(entry("z", 0), entry("y", 1));
        assertThat(maps.get(1)).isNull();
    }

    @Test
    public void testWriteColsRecordDeltaColumns() throws Exception {
        createTableDefault();
        writeBase(Arrays.asList(map("a", 1), map("a", 2)));

        // a single write may update a column whole and merge into a map
        RowType writeType = getTableDefault().rowType().project(Arrays.asList("m", "d"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType)) {
            ((TableWriteImpl<?>) write).withMapDeltaColumns(Collections.singletonList("m"));
            write.write(GenericRow.of(toGenericMap(map("b", 3)), 100));
            write.write(GenericRow.of(toGenericMap(map()), 200));
            List<CommitMessage> messages = write.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }

        assertThat(dataFiles())
                .extracting(DataFileMeta::writeCols)
                .containsExactlyInAnyOrder(null, Arrays.asList("m", "d", "_MAP_DELTA_m"));
        assertThat(readMaps()).containsExactly(map("a", 1, "b", 3), map("a", 2));
        List<InternalRow> rows = readRows(getTableDefault().rowType());
        assertThat(rows.get(0).getInt(3)).isEqualTo(100);
        assertThat(rows.get(1).getInt(3)).isEqualTo(200);
    }

    @Test
    public void testProjectionOpensOnlyWhatItNeeds() throws Exception {
        createTableDefault();
        writeBase(Arrays.asList(map("a", 1), map("a", 2)));
        writeDelta(Arrays.asList(map("b", 3), map()));

        RowType full = getTableDefault().rowType();
        // a projection without the map does not need the deltas
        List<InternalRow> ids = readRows(full.project(Arrays.asList("id", "c")));
        assertThat(ids).extracting(r -> r.getInt(0)).containsExactly(0, 1);

        // the map alone, and the map with columns in a different order
        List<InternalRow> onlyMap = readRows(full.project(Collections.singletonList("m")));
        assertThat(onlyMap)
                .extracting(r -> toJavaMap(r.getMap(0)))
                .containsExactly(map("a", 1, "b", 3), map("a", 2));
        List<InternalRow> reordered = readRows(full.project(Arrays.asList("d", "m", "id")));
        assertThat(reordered).extracting(r -> r.getInt(0)).containsExactly(0, 10);
        assertThat(reordered)
                .extracting(r -> toJavaMap(r.getMap(1)))
                .containsExactly(map("a", 1, "b", 3), map("a", 2));
        assertThat(reordered).extracting(r -> r.getInt(2)).containsExactly(0, 1);
    }

    @Test
    public void testCompactionMaterializesDeltas() throws Exception {
        createTableDefault();
        int n = 20;
        List<Map<String, Integer>> base = new ArrayList<>();
        List<Map<String, Integer>> delta1 = new ArrayList<>();
        List<Map<String, Integer>> delta2 = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            base.add(i % 5 == 0 ? null : map("k" + i, i));
            delta1.add(i % 2 == 0 ? map("even", i) : map());
            delta2.add(i % 3 == 0 ? map("k" + i, -i) : map());
        }
        writeBase(base);
        writeDelta(delta1);
        writeDelta(delta2);
        List<Map<String, Integer>> expected = readMaps();

        compact();

        List<DataFileMeta> files = dataFiles();
        assertThat(files).hasSize(1);
        assertThat(files.get(0).writeCols()).isNull();
        assertThat(readMaps()).isEqualTo(expected);
        for (int i = 0; i < n; i++) {
            if (i % 5 == 0) {
                assertThat(expected.get(i)).isNull();
                continue;
            }
            Map<String, Integer> value = expected.get(i);
            assertThat(value.get("k" + i)).isEqualTo(i % 3 == 0 ? -i : i);
            assertThat(value.containsKey("even")).isEqualTo(i % 2 == 0);
        }
        assertThat(readRowIds()).containsExactly(rowIds(n));
    }

    @Test
    public void testDeltaWithoutBaseReadsNull() throws Exception {
        // the map column is added after the base rows were written, they read it as NULL
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(GenericRow.of(0));
            write.write(GenericRow.of(1));
            commit(builder, write.prepareCommit());
        }
        catalog.alterTable(
                identifier(),
                SchemaChange.addColumn("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                false);

        writeDelta(Arrays.asList(map("a", 1), map()));

        assertThat(readMaps(1)).containsExactly(null, null);
        // the delta alone is read through the merge path as well
        List<InternalRow> onlyMap =
                readRows(getTableDefault().rowType().project(Collections.singletonList("m")));
        assertThat(onlyMap).extracting(r -> r.isNullAt(0)).containsExactly(true, true);
    }

    @Test
    public void testBinaryKeysAreMergedByContent() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("m", DataTypes.MAP(DataTypes.BYTES(), DataTypes.INT()))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(GenericRow.of(0, bytesMap(new byte[] {1}, 1, new byte[] {2}, 2)));
            commit(builder, write.prepareCommit());
        }
        writeDelta(Collections.singletonList(bytesMap(new byte[] {1}, 10, new byte[] {3}, 3)));

        InternalMap merged = readRows(getTableDefault().rowType()).get(0).getMap(1);
        assertThat(merged.size()).isEqualTo(3);
        InternalArray keys = merged.keyArray();
        InternalArray values = merged.valueArray();
        assertThat(keys.getBinary(0)).containsExactly(1);
        assertThat(values.getInt(0)).isEqualTo(10);
        assertThat(keys.getBinary(1)).containsExactly(2);
        assertThat(values.getInt(1)).isEqualTo(2);
        assertThat(keys.getBinary(2)).containsExactly(3);
        assertThat(values.getInt(2)).isEqualTo(3);
    }

    @Test
    public void testStatsOfDeltaDoNotPruneMergedValues() throws Exception {
        createTableDefault();
        // every base value is NULL, the deltas keep them NULL but are themselves never NULL
        writeBase(Arrays.asList(null, null, null));
        writeDelta(Arrays.asList(map("a", 1), map(), map("b", 2)));

        PredicateBuilder builder = new PredicateBuilder(getTableDefault().rowType());
        assertThat(readMaps(filtered(builder.isNull(2)))).containsExactly(null, null, null);

        // the opposite case: the base is never NULL, a delta sets every value to NULL
        dropTableDefault();
        createTableDefault();
        writeBase(Arrays.asList(map("a", 1), map("a", 1)));
        writeDelta(Arrays.asList(null, null));
        assertThat(readMaps(filtered(builder.isNull(2)))).containsExactly(null, null);
    }

    @Test
    public void testFilesTableStatsOfDeltaFile() throws Exception {
        createTableDefault();
        // store the stats of every written column, positionally matched to the write columns
        catalog.alterTable(
                identifier(),
                SchemaChange.setOption(CoreOptions.METADATA_STATS_DENSE_STORE.key(), "false"),
                false);
        writeBase(Arrays.asList(map("a", 1), map("a", 2)));

        // the stats are those of the physical columns, the marker after them has none
        RowType writeType = getTableDefault().rowType().project(Arrays.asList("m", "d"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType)) {
            ((TableWriteImpl<?>) write).withMapDeltaColumns(Collections.singletonList("m"));
            write.write(GenericRow.of(toGenericMap(map("b", 3)), null));
            write.write(GenericRow.of(toGenericMap(map()), null));
            List<CommitMessage> messages = write.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }

        List<InternalRow> files = readFilesTable();
        InternalRow deltaFile =
                files.stream()
                        .filter(r -> !r.isNullAt(writeColsIndex(r)))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        InternalArray writeCols = deltaFile.getArray(writeColsIndex(deltaFile));
        assertThat(writeCols.size()).isEqualTo(3);
        assertThat(writeCols.getString(0).toString()).isEqualTo("m");
        assertThat(writeCols.getString(2).toString()).isEqualTo("_MAP_DELTA_m");
        // d is NULL in both rows of the delta file
        assertThat(nullValueCounts(deltaFile)).contains(" d=2");
    }

    @Test
    public void testNestedFieldAndMapDeltaTogether() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "nest",
                                DataTypes.ROW(
                                        DataTypes.FIELD(10, "a", DataTypes.INT()),
                                        DataTypes.FIELD(11, "b", DataTypes.INT())))
                        .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        RowType full = getTableDefault().rowType();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(GenericRow.of(0, GenericRow.of(1, 2), toGenericMap(map("a", 1))));
            commit(builder, write.prepareCommit());
        }

        RowType writeType = full.projectByPaths(Arrays.asList("nest.a", "m"));
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType)) {
            ((TableWriteImpl<?>) write).withMapDeltaColumns(Collections.singletonList("m"));
            write.write(GenericRow.of(GenericRow.of(100), toGenericMap(map("b", 2))));
            List<CommitMessage> messages = write.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }

        assertThat(dataFiles())
                .extracting(DataFileMeta::writeCols)
                .containsExactlyInAnyOrder(null, Arrays.asList("nest.a", "m", "_MAP_DELTA_m"));
        InternalRow row = readRows(full).get(0);
        assertThat(row.getRow(1, 2).getInt(0)).isEqualTo(100);
        assertThat(row.getRow(1, 2).getInt(1)).isEqualTo(2);
        assertThat(toJavaMap(row.getMap(2))).isEqualTo(map("a", 1, "b", 2));
    }

    @Test
    public void testWriteRequiresTheOption() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        RowType writeType = table.rowType().project(Collections.singletonList("m"));
        try (BatchTableWrite write =
                table.newBatchWriteBuilder().newWrite().withWriteType(writeType)) {
            assertThatThrownBy(
                            () ->
                                    ((TableWriteImpl<?>) write)
                                            .withMapDeltaColumns(Collections.singletonList("m")))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key());
            // no map delta column is always fine
            ((TableWriteImpl<?>) write).withMapDeltaColumns(Collections.emptyList());
        }

        // the table must record that it may contain map deltas, a dynamic option cannot enable it
        assertThatThrownBy(
                        () ->
                                table.copy(
                                        Collections.singletonMap(
                                                CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(),
                                                "true")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("can only be enabled by altering the table");
    }

    @Test
    public void testInvalidDeltaColumnsAreRejected() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        RowType writeType = table.rowType().project(Arrays.asList("c", "m"));
        try (BatchTableWrite write =
                table.newBatchWriteBuilder().newWrite().withWriteType(writeType)) {
            TableWriteImpl<?> impl = (TableWriteImpl<?>) write;
            // not a map
            assertThatThrownBy(() -> impl.withMapDeltaColumns(Collections.singletonList("c")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'c'");
            // not written
            assertThatThrownBy(() -> impl.withMapDeltaColumns(Collections.singletonList("d")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'d'");
            // unknown
            assertThatThrownBy(() -> impl.withMapDeltaColumns(Collections.singletonList("x")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'x'");
        }
    }

    @Test
    public void testDeltaCannotBeWrittenWithDedicatedFileColumns() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .column("b", DataTypes.BLOB())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        try (BatchTableWrite write =
                table.newBatchWriteBuilder()
                        .newWrite()
                        .withWriteType(table.rowType().project(Arrays.asList("m", "b")))) {
            assertThatThrownBy(
                            () ->
                                    ((TableWriteImpl<?>) write)
                                            .withMapDeltaColumns(Collections.singletonList("m")))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("dedicated blob or vector files");
        }
        // without the blob column the map delta can be written
        try (BatchTableWrite write =
                table.newBatchWriteBuilder()
                        .newWrite()
                        .withWriteType(table.rowType().project(Collections.singletonList("m")))) {
            ((TableWriteImpl<?>) write).withMapDeltaColumns(Collections.singletonList("m"));
        }

        // a descriptor blob is stored inline in the normal file, so it can be written with it
        dropTableDefault();
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .column("b", DataTypes.BLOB())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true")
                        .option(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "b")
                        .build(),
                false);
        table = getTableDefault();
        try (BatchTableWrite write =
                table.newBatchWriteBuilder()
                        .newWrite()
                        .withWriteType(table.rowType().project(Arrays.asList("m", "b")))) {
            ((TableWriteImpl<?>) write).withMapDeltaColumns(Collections.singletonList("m"));
        }
    }

    @Test
    public void testIncrementalReadWithoutWholeValueFails() throws Exception {
        createTableDefault();
        writeBase(Arrays.asList(map("a", 1), map("a", 2)));
        long baseSnapshot = getTableDefault().latestSnapshot().get().id();
        writeDelta(Arrays.asList(map("b", 3), map()));
        long deltaSnapshot = getTableDefault().latestSnapshot().get().id();

        // the changes of the base snapshot can be read
        assertThat(readDelta(baseSnapshot, getTableDefault().rowType())).hasSize(2);
        // the changes of the delta snapshot hold only merged entries of m
        assertThatThrownBy(() -> readDelta(deltaSnapshot, getTableDefault().rowType()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot read map column 'm' incrementally");
        // without m they can be read
        assertThat(
                        readDelta(
                                deltaSnapshot,
                                getTableDefault().rowType().project(Arrays.asList("id", "c"))))
                .hasSize(2);
        // a batch read merges them
        assertThat(readMaps()).containsExactly(map("a", 1, "b", 3), map("a", 2));
    }

    private List<InternalRow> readDelta(long snapshotId, RowType readType) throws Exception {
        FileStoreTable table = getTableDefault();
        List<Split> splits =
                new ArrayList<>(
                        table.newSnapshotReader()
                                .withMode(ScanMode.DELTA)
                                .withSnapshot(snapshotId)
                                .read()
                                .splits());
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder().withReadType(readType).newRead().createReader(splits)) {
            reader.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    public void testKeysWithoutValueEqualityAreRejected() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dm", DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.INT()))
                        .column(
                                "rm",
                                DataTypes.MAP(
                                        DataTypes.ROW(DataTypes.FIELD(10, "a", DataTypes.INT())),
                                        DataTypes.INT()))
                        .column("sm", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        try (BatchTableWrite write =
                table.newBatchWriteBuilder()
                        .newWrite()
                        .withWriteType(table.rowType().project(Arrays.asList("dm", "rm", "sm")))) {
            TableWriteImpl<?> impl = (TableWriteImpl<?>) write;
            // engines normalize -0.0 and NaN keys
            assertThatThrownBy(() -> impl.withMapDeltaColumns(Collections.singletonList("dm")))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("'dm' has keys of type DOUBLE");
            assertThatThrownBy(() -> impl.withMapDeltaColumns(Collections.singletonList("rm")))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("'rm' has keys of type ROW");
            impl.withMapDeltaColumns(Collections.singletonList("sm"));
        }
    }

    @Test
    public void testNewWriteTypeClearsDeltaColumns() throws Exception {
        createTableDefault();
        writeBase(Arrays.asList(map("a", 1), map("a", 2)));
        RowType writeType = getTableDefault().rowType().project(Collections.singletonList("m"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType)) {
            ((TableWriteImpl<?>) write).withMapDeltaColumns(Collections.singletonList("m"));
            // a later write type is written whole unless its delta columns are given again
            write.withWriteType(writeType);
            write.write(GenericRow.of(toGenericMap(map("z", 0))));
            write.write(GenericRow.of(toGenericMap(map("z", 0))));
            List<CommitMessage> messages = write.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }
        assertThat(readMaps()).containsExactly(map("z", 0), map("z", 0));
    }

    @Test
    public void testOptionCannotBeDisabledOrRemovedAfterWrite() throws Exception {
        createTableDefault();
        writeBase(Collections.singletonList(map("a", 1)));
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier(),
                                        SchemaChange.setOption(
                                                CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(),
                                                "false"),
                                        false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key());
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier(),
                                        SchemaChange.removeOption(
                                                CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key()),
                                        false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key());
        assertThatThrownBy(
                        () ->
                                getTableDefault()
                                        .copy(
                                                Collections.singletonMap(
                                                        CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED
                                                                .key(),
                                                        "false")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key());
    }

    @Test
    public void testOptionCanBePersistentlyEnabledAfterWrite() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(GenericRow.of(0, toGenericMap(map("a", 1))));
            commit(builder, write.prepareCommit());
        }
        // a reader created before the option is enabled, as a long-running engine may keep it
        ReadBuilder staleReadBuilder = table.newReadBuilder();

        catalog.alterTable(
                identifier(),
                SchemaChange.setOption(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true"),
                false);
        writeDelta(Collections.singletonList(map("b", 2)));

        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                staleReadBuilder.newRead().createReader(staleReadBuilder.newScan().plan())) {
            reader.forEachRemaining(rows::add);
        }
        assertThat(rows).hasSize(1);
        assertThat(toJavaMap(rows.get(0).getMap(1))).isEqualTo(map("a", 1, "b", 2));
    }

    @Test
    public void testLiteralPrefixedColumnKeepsItsMeaning() throws Exception {
        // without the option a column may be named like a map delta, it is written whole
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .column("_MAP_DELTA_m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(GenericRow.of(0, toGenericMap(map("a", 1)), toGenericMap(map("x", 1))));
            commit(builder, write.prepareCommit());
        }
        writeColumns(
                Collections.singletonList("_MAP_DELTA_m"),
                Collections.singletonList(GenericRow.of(toGenericMap(map("y", 2)))));

        InternalRow row = readRows(getTableDefault().rowType()).get(0);
        assertThat(toJavaMap(row.getMap(1))).isEqualTo(map("a", 1));
        assertThat(toJavaMap(row.getMap(2))).isEqualTo(map("y", 2));

        // also when written together with the map it looks like the marker of
        writeColumns(
                Arrays.asList("m", "_MAP_DELTA_m"),
                Collections.singletonList(
                        GenericRow.of(toGenericMap(map("b", 3)), toGenericMap(map("z", 4)))));
        row = readRows(getTableDefault().rowType()).get(0);
        assertThat(toJavaMap(row.getMap(1))).isEqualTo(map("b", 3));
        assertThat(toJavaMap(row.getMap(2))).isEqualTo(map("z", 4));

        // and the option cannot be enabled while the names are ambiguous
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier(),
                                        SchemaChange.setOption(
                                                CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(),
                                                "true"),
                                        false))
                .hasStackTraceContaining(
                        CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key()
                                + " cannot be enabled for a table containing both the map column "
                                + "'m' and a column named '_MAP_DELTA_m'");
    }

    @Test
    public void testAlterCannotCreateAmbiguousDeltaNames() throws Exception {
        createTableDefault();
        writeBase(Arrays.asList(map("a", 1)));
        String message = "a column named '_MAP_DELTA_m'";
        // a column named like a map delta of an existing map
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier(),
                                        SchemaChange.addColumn("_MAP_DELTA_m", DataTypes.INT()),
                                        false))
                .hasStackTraceContaining(message);
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier(),
                                        SchemaChange.renameColumn("c", "_MAP_DELTA_m"),
                                        false))
                .hasStackTraceContaining(message);
        // a map named after an existing column of that form
        catalog.alterTable(
                identifier(), SchemaChange.addColumn("_MAP_DELTA_n", DataTypes.INT()), false);
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier(),
                                        SchemaChange.addColumn(
                                                "n",
                                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                                        false))
                .hasStackTraceContaining("a column named '_MAP_DELTA_n'");
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier(), SchemaChange.renameColumn("m", "n"), false))
                .hasStackTraceContaining("a column named '_MAP_DELTA_n'");
        // nothing changed the map deltas of m
        assertThat(readMaps()).containsExactly(map("a", 1));
    }

    @Test
    public void testOptionRequiresDataEvolution() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .option(CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true")
                        .build();
        assertThatThrownBy(() -> catalog.createTable(identifier(), schema, false))
                .hasStackTraceContaining(
                        CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED.key()
                                + " requires "
                                + CoreOptions.DATA_EVOLUTION_ENABLED.key()
                                + "=true.");
    }

    // ------------------------------------------------------------------------------------------

    private void writeBase(List<Map<String, Integer>> maps) throws Exception {
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (int i = 0; i < maps.size(); i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("c" + i),
                                toGenericMap(maps.get(i)),
                                i * 10));
            }
            commit(builder, write.prepareCommit());
        }
    }

    private void writeDelta(List<?> deltas) throws Exception {
        FileStoreTable table = getTableDefault();
        RowType writeType = table.rowType().project(Collections.singletonList("m"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType)) {
            ((TableWriteImpl<?>) write).withMapDeltaColumns(Collections.singletonList("m"));
            for (Object delta : deltas) {
                write.write(
                        GenericRow.of(
                                delta instanceof GenericMap
                                        ? delta
                                        : toGenericMap(castMap(delta))));
            }
            List<CommitMessage> messages = write.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }
    }

    private void writeColumns(List<String> columns, List<GenericRow> rows) throws Exception {
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                builder.newWrite().withWriteType(table.rowType().project(columns))) {
            for (GenericRow row : rows) {
                write.write(convertMaps(row));
            }
            List<CommitMessage> messages = write.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }
    }

    private void compact() throws Exception {
        FileStoreTable table =
                getTableDefault()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2"));
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(
                        table, false, false, table.latestSnapshot().get());
        List<CommitMessage> messages = new ArrayList<>();
        try {
            List<DataEvolutionCompactTask> tasks;
            while (!(tasks = coordinator.plan()).isEmpty()) {
                for (DataEvolutionCompactTask task : tasks) {
                    messages.add(task.doCompact(table, "map-delta-compact"));
                }
            }
        } catch (EndOfScanException ignore) {
        }
        assertThat(messages).isNotEmpty();
        commit(table.newBatchWriteBuilder(), messages);
    }

    private void commit(BatchWriteBuilder builder, List<CommitMessage> messages) throws Exception {
        try (BatchTableCommit commit = builder.newCommit()) {
            commit.commit(messages);
        }
    }

    private List<DataFileMeta> dataFiles() throws Exception {
        List<DataFileMeta> files = new ArrayList<>();
        for (Split split : getTableDefault().newReadBuilder().newScan().plan().splits()) {
            files.addAll(((DataSplit) split).dataFiles());
        }
        return files;
    }

    private List<InternalRow> readRows(RowType readType) throws Exception {
        return readRows(readType, rb -> {});
    }

    private List<InternalRow> readRows(RowType readType, Consumer<ReadBuilder> configure)
            throws Exception {
        ReadBuilder readBuilder = getTableDefault().newReadBuilder().withReadType(readType);
        configure.accept(readBuilder);
        List<InternalRow> rows = new ArrayList<>();
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[readType.getFieldCount()];
        for (int i = 0; i < getters.length; i++) {
            getters[i] = InternalRow.createFieldGetter(readType.getTypeAt(i), i);
        }
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        // copy the row, the reader reuses it
                        Object[] values = new Object[getters.length];
                        for (int i = 0; i < getters.length; i++) {
                            values[i] = getters[i].getFieldOrNull(row);
                        }
                        rows.add(GenericRow.of(values));
                    });
        }
        return rows;
    }

    private List<Map<String, Integer>> readMaps() throws Exception {
        return readMaps(2);
    }

    private List<Map<String, Integer>> readMaps(int mapIndex) throws Exception {
        return readMaps(mapIndex, rb -> {});
    }

    private List<Map<String, Integer>> readMaps(Consumer<ReadBuilder> configure) throws Exception {
        return readMaps(2, configure);
    }

    private List<Map<String, Integer>> readMaps(int mapIndex, Consumer<ReadBuilder> configure)
            throws Exception {
        List<Map<String, Integer>> maps = new ArrayList<>();
        for (InternalRow row : readRows(getTableDefault().rowType(), configure)) {
            maps.add(row.isNullAt(mapIndex) ? null : toJavaMap(row.getMap(mapIndex)));
        }
        return maps;
    }

    private static Consumer<ReadBuilder> filtered(Predicate predicate) {
        return rb -> rb.withFilter(predicate);
    }

    private List<Long> readRowIds() throws Exception {
        List<Long> ids = new ArrayList<>();
        for (InternalRow row : readRows(RowType.of(SpecialFields.ROW_ID))) {
            ids.add(row.getLong(0));
        }
        return ids;
    }

    private static Long[] rowIds(int n) {
        Long[] ids = new Long[n];
        for (int i = 0; i < n; i++) {
            ids[i] = (long) i;
        }
        return ids;
    }

    private List<InternalRow> readFilesTable() throws Exception {
        FileStoreTable table = getTableDefault();
        FilesTable filesTable = new FilesTable(table);
        ReadBuilder readBuilder = filesTable.newReadBuilder();
        List<InternalRow> rows = new ArrayList<>();
        RowType rowType = filesTable.rowType();
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < getters.length; i++) {
            getters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        Object[] values = new Object[getters.length];
                        for (int i = 0; i < getters.length; i++) {
                            values[i] = getters[i].getFieldOrNull(row);
                        }
                        rows.add(GenericRow.of(values));
                    });
        }
        return rows;
    }

    private static int writeColsIndex(InternalRow ignored) {
        return FilesTable.TABLE_TYPE.getFieldIndex("write_cols");
    }

    private static String nullValueCounts(InternalRow filesRow) {
        return filesRow.getString(FilesTable.TABLE_TYPE.getFieldIndex("null_value_counts"))
                .toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Integer> castMap(Object value) {
        return (Map<String, Integer>) value;
    }

    private static GenericRow convertMaps(GenericRow row) {
        Object[] values = new Object[row.getFieldCount()];
        for (int i = 0; i < values.length; i++) {
            Object value = row.getField(i);
            values[i] = value instanceof Map ? toGenericMap(castMap(value)) : value;
        }
        return GenericRow.of(values);
    }

    @Nullable
    private static GenericMap toGenericMap(@Nullable Map<String, Integer> map) {
        if (map == null) {
            return null;
        }
        Map<Object, Object> converted = new LinkedHashMap<>();
        map.forEach((k, v) -> converted.put(BinaryString.fromString(k), v));
        return new GenericMap(converted);
    }

    private static Map<String, Integer> toJavaMap(InternalMap map) {
        Map<String, Integer> result = new LinkedHashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.getString(i).toString(), values.isNullAt(i) ? null : values.getInt(i));
        }
        return result;
    }

    private static GenericMap bytesMap(byte[] k1, int v1, byte[] k2, int v2) {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return new GenericMap(map);
    }

    private static Map<String, Integer> map(Object... kvs) {
        Map<String, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put((String) kvs[i], (Integer) kvs[i + 1]);
        }
        return map;
    }

    private static Map<String, Integer> mapWithNullValue(String key) {
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put(key, null);
        return map;
    }

    private static Map.Entry<String, Integer> entry(String key, @Nullable Integer value) {
        return new java.util.AbstractMap.SimpleEntry<>(key, value);
    }
}
