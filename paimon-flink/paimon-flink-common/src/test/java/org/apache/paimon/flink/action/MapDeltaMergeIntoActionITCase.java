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

package org.apache.paimon.flink.action;

import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_MAP_DELTA_ENABLED;
import static org.apache.paimon.CoreOptions.ROW_TRACKING_ENABLED;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildDdl;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ITCase for key-level data evolution of map columns via {@link DataEvolutionMergeIntoAction}: the
 * set value of a map-delta column is merged into the current map, and only the merged entries are
 * written, as a map-delta file aligned by row id.
 */
public class MapDeltaMergeIntoActionITCase extends ActionITCaseBase {

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();
        init(warehouse);
    }

    private void prepareTarget(boolean mapDeltaEnabled) throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("id INT", "c STRING", "m MAP<STRING, INT>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                if (mapDeltaEnabled) {
                                    put(DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true");
                                }
                            }
                        }));
        insertInto(
                "T",
                "(1, 'a', MAP['x', 1, 'y', 2])",
                "(2, 'b', MAP['x', 2])",
                "(3, 'c', CAST(NULL AS MAP<STRING, INT>))",
                "(4, 'd', MAP['x', 4])");
    }

    private void prepareSource(String... values) throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "c STRING", "d MAP<STRING, INT>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>()));
        insertInto("S", values);
    }

    @Test
    public void testMergesSetValueIntoTheMap() throws Exception {
        prepareTarget(true);
        prepareSource(
                "(1, 'A', MAP['y', 20, 'z', 30])",
                "(3, 'C', MAP['x', 3])",
                "(4, 'D', CAST(NULL AS MAP<STRING, INT>))");

        builder()
                .withMatchedUpdateSet("m=S.d")
                .withMapDeltaColumns("m")
                .withSinkParallelism(2)
                .build()
                .run();

        // a NULL map stays NULL and a NULL value sets the map to NULL, like map_concat
        testBatchRead(
                "SELECT id, c, m FROM T",
                Arrays.asList(
                        changelogRow("+I", 1, "a", map("x", 1, "y", 20, "z", 30)),
                        changelogRow("+I", 2, "b", map("x", 2)),
                        changelogRow("+I", 3, "c", null),
                        changelogRow("+I", 4, "d", null)));
        assertThat(writeCols()).contains(Arrays.asList("m", "_MAP_DELTA_m"));
    }

    @Test
    public void testMapDeltaWithOtherColumns() throws Exception {
        prepareTarget(true);
        prepareSource("(1, 'A', MAP['y', 20])", "(2, 'B', MAP['x', 20])");

        builder()
                .withMatchedUpdateSet("c=S.c,m=S.d")
                .withMapDeltaColumns(" m ")
                .withSinkParallelism(1)
                .build()
                .run();

        testBatchRead(
                "SELECT id, c, m FROM T",
                Arrays.asList(
                        changelogRow("+I", 1, "A", map("x", 1, "y", 20)),
                        changelogRow("+I", 2, "B", map("x", 20)),
                        changelogRow("+I", 3, "c", null),
                        changelogRow("+I", 4, "d", map("x", 4))));
        assertThat(writeCols())
                .anySatisfy(
                        cols ->
                                assertThat(cols)
                                        .containsExactlyInAnyOrder("c", "m", "_MAP_DELTA_m"));
    }

    @Test
    public void testSuccessiveMergesAccumulate() throws Exception {
        prepareTarget(true);
        prepareSource("(1, 'A', MAP['k1', 1])", "(4, 'D', MAP['k1', 1])");
        builder().withMatchedUpdateSet("m=S.d").withMapDeltaColumns("m").build().run();

        sEnv.executeSql("DROP TABLE S");
        prepareSource("(1, 'A', MAP['k2', 2])", "(2, 'B', MAP['k1', 9])");
        builder().withMatchedUpdateSet("m=S.d").withMapDeltaColumns("m").build().run();

        testBatchRead(
                "SELECT id, m FROM T",
                Arrays.asList(
                        changelogRow("+I", 1, map("x", 1, "y", 2, "k1", 1, "k2", 2)),
                        changelogRow("+I", 2, map("x", 2, "k1", 9)),
                        changelogRow("+I", 3, null),
                        changelogRow("+I", 4, map("x", 4, "k1", 1))));
    }

    @Test
    public void testProcedure() throws Exception {
        prepareTarget(true);
        prepareSource("(2, 'B', MAP['y', 2])");

        try (CloseableIterator<Row> ignored =
                executeSQL(
                        String.format(
                                "CALL sys.data_evolution_merge_into('%s.T', '', '', 'S', "
                                        + "'T.id=S.id', 'm=S.d', 2, 'm')",
                                database),
                        false,
                        true)) {
            // wait for the job
        }

        testBatchRead(
                "SELECT id, m FROM T",
                Arrays.asList(
                        changelogRow("+I", 1, map("x", 1, "y", 2)),
                        changelogRow("+I", 2, map("x", 2, "y", 2)),
                        changelogRow("+I", 3, null),
                        changelogRow("+I", 4, map("x", 4))));
        assertThat(writeCols()).contains(Arrays.asList("m", "_MAP_DELTA_m"));
    }

    @Test
    public void testWithoutMapDeltaColumnsTheMapIsReplaced() throws Exception {
        prepareTarget(true);
        prepareSource("(1, 'A', MAP['y', 20])");

        builder().withMatchedUpdateSet("m=S.d").build().run();

        testBatchRead(
                "SELECT id, m FROM T WHERE id = 1",
                Collections.singletonList(changelogRow("+I", 1, map("y", 20))));
        assertThat(writeCols()).contains(Collections.singletonList("m"));
    }

    @Test
    public void testUpdateAllColumns() throws Exception {
        prepareTarget(true);
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "c STRING", "m MAP<STRING, INT>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>()));
        insertInto("S", "(1, 'A', MAP['z', 26])");

        builder().withMatchedUpdateSet("*").withMapDeltaColumns("m").build().run();

        testBatchRead(
                "SELECT id, c, m FROM T",
                Arrays.asList(
                        changelogRow("+I", 1, "A", map("x", 1, "y", 2, "z", 26)),
                        changelogRow("+I", 2, "b", map("x", 2)),
                        changelogRow("+I", 3, "c", null),
                        changelogRow("+I", 4, "d", map("x", 4))));
    }

    @Test
    public void testRejectsFloatingPointKeys() throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("id INT", "m MAP<DOUBLE, INT>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_MAP_DELTA_ENABLED.key(), "true");
                            }
                        }));
        insertInto("T", "(1, MAP[1.5, 1])");
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "d MAP<DOUBLE, INT>"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>()));
        insertInto("S", "(1, MAP[2.5, 2])");

        assertThatThrownBy(
                        () ->
                                builder()
                                        .withMatchedUpdateSet("m=S.d")
                                        .withMapDeltaColumns("m")
                                        .build()
                                        .run())
                .hasMessageContaining("'m' has keys of type DOUBLE");
    }

    @Test
    public void testRequiresTheOption() throws Exception {
        prepareTarget(false);
        prepareSource("(1, 'A', MAP['y', 20])");

        assertThatThrownBy(
                        () ->
                                builder()
                                        .withMatchedUpdateSet("m=S.d")
                                        .withMapDeltaColumns("m")
                                        .build()
                                        .run())
                .hasMessageContaining(DATA_EVOLUTION_MAP_DELTA_ENABLED.key());
    }

    @Test
    public void testRejectsInvalidMapDeltaColumns() throws Exception {
        prepareTarget(true);
        prepareSource("(1, 'A', MAP['y', 20])");

        // not a map
        assertThatThrownBy(
                        () ->
                                builder()
                                        .withMatchedUpdateSet("c=S.c")
                                        .withMapDeltaColumns("c")
                                        .build()
                                        .run())
                .hasMessageContaining("Map delta column 'c' is not a map column");
        // not updated
        assertThatThrownBy(
                        () ->
                                builder()
                                        .withMatchedUpdateSet("c=S.c")
                                        .withMapDeltaColumns("m")
                                        .build()
                                        .run())
                .hasMessageContaining("Map delta column 'm' is not a column updated");
    }

    private static Map<String, Integer> map(Object... kvs) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put((String) kvs[i], (Integer) kvs[i + 1]);
        }
        return map;
    }

    /** The write columns of every data file in the latest snapshot of the target table. */
    private List<List<String>> writeCols() throws Exception {
        FileStoreTable table = getFileStoreTable("T");
        List<List<String>> result = new ArrayList<>();
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            result.add(entry.file().writeCols());
        }
        return result;
    }

    private Builder builder() {
        return new Builder(warehouse, database);
    }

    private static class Builder {
        private final List<String> args;

        Builder(String warehouse, String database) {
            this.args =
                    new ArrayList<>(
                            Arrays.asList(
                                    "data_evolution_merge_into",
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database,
                                    "--table",
                                    "T",
                                    "--source_table",
                                    "S",
                                    "--on",
                                    "T.id=S.id",
                                    "--sink_parallelism",
                                    "2"));
        }

        Builder withMatchedUpdateSet(String matchedUpdateSet) {
            args.add("--matched_update_set");
            args.add(matchedUpdateSet);
            return this;
        }

        Builder withMapDeltaColumns(String mapDeltaColumns) {
            args.add("--map_delta_columns");
            args.add(mapDeltaColumns);
            return this;
        }

        Builder withSinkParallelism(int sinkParallelism) {
            int index = args.indexOf("--sink_parallelism");
            args.set(index + 1, String.valueOf(sinkParallelism));
            return this;
        }

        DataEvolutionMergeIntoAction build() {
            return (DataEvolutionMergeIntoAction)
                    ActionFactory.createAction(args.toArray(new String[0]))
                            .orElseThrow(RuntimeException::new);
        }
    }
}
