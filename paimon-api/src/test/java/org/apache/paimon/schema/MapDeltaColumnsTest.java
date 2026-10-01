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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MapDeltaColumns}. */
class MapDeltaColumnsTest {

    private static final List<DataField> FIELDS =
            Arrays.asList(
                    new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                    new DataField(2, "a.b", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                    new DataField(3, "s", DataTypes.STRING()),
                    new DataField(
                            4, "_MAP_DELTA_x", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                    new DataField(5, "x", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())));
    private static final RowType ROW_TYPE = new RowType(FIELDS);

    @Test
    void testEncodeAndDecode() {
        assertThat(MapDeltaColumns.encode("m")).isEqualTo("_MAP_DELTA_m");
        assertThat(MapDeltaColumns.decode(ROW_TYPE, "_MAP_DELTA_m")).isEqualTo("m");
        // a dot in the column name is part of the name
        assertThat(MapDeltaColumns.decode(ROW_TYPE, "_MAP_DELTA_a.b")).isEqualTo("a.b");
    }

    @Test
    void testDecodeOnlyMapDeltas() {
        // a plain column
        assertThat(MapDeltaColumns.decode(ROW_TYPE, "m")).isNull();
        // not a map
        assertThat(MapDeltaColumns.decode(ROW_TYPE, "_MAP_DELTA_s")).isNull();
        // unknown column
        assertThat(MapDeltaColumns.decode(ROW_TYPE, "_MAP_DELTA_missing")).isNull();
        assertThat(MapDeltaColumns.decode(ROW_TYPE, "_MAP_DELTA_")).isNull();
        // a column literally named like a map delta keeps its meaning
        assertThat(MapDeltaColumns.decode(ROW_TYPE, "_MAP_DELTA_x")).isNull();
    }

    @Test
    void testToPhysical() {
        List<String> plain = Arrays.asList("id", "m");
        assertThat(MapDeltaColumns.toPhysical(ROW_TYPE, plain)).isSameAs(plain);
        assertThat(MapDeltaColumns.toPhysical(FIELDS, plain)).isSameAs(plain);

        List<String> withDelta = Arrays.asList("id", "m", "_MAP_DELTA_x", "s", "_MAP_DELTA_m");
        assertThat(MapDeltaColumns.toPhysical(ROW_TYPE, withDelta))
                .containsExactly("id", "m", "_MAP_DELTA_x", "s");
        assertThat(MapDeltaColumns.toPhysical(FIELDS, withDelta))
                .containsExactly("id", "m", "_MAP_DELTA_x", "s");
        // the input is not modified
        assertThat(withDelta).containsExactly("id", "m", "_MAP_DELTA_x", "s", "_MAP_DELTA_m");

        // a marker name whose column the write columns do not list is not a marker
        List<String> withoutColumn = Arrays.asList("id", "_MAP_DELTA_m");
        assertThat(MapDeltaColumns.toPhysical(ROW_TYPE, withoutColumn)).isSameAs(withoutColumn);
    }

    @Test
    void testDeltaColumnsAndFieldIds() {
        assertThat(MapDeltaColumns.deltaColumns(ROW_TYPE, null)).isEmpty();
        assertThat(MapDeltaColumns.deltaColumns(ROW_TYPE, Arrays.asList("m", "x"))).isEmpty();
        List<String> writeCols = Arrays.asList("a.b", "m", "s", "_MAP_DELTA_a.b", "_MAP_DELTA_m");
        assertThat(MapDeltaColumns.deltaColumns(ROW_TYPE, writeCols)).containsExactly("a.b", "m");
        assertThat(MapDeltaColumns.deltaFieldIds(ROW_TYPE, writeCols)).containsExactly(1, 2);
        assertThat(MapDeltaColumns.deltaFieldIds(ROW_TYPE, Collections.singletonList("m")))
                .isEmpty();
        // only a marker whose column is written counts
        assertThat(MapDeltaColumns.deltaColumns(ROW_TYPE, Arrays.asList("s", "_MAP_DELTA_m")))
                .isEmpty();
        assertThat(
                        MapDeltaColumns.deltaColumns(
                                ROW_TYPE, Arrays.asList("m", "_MAP_DELTA_m", "_MAP_DELTA_a.b")))
                .containsExactly("m");
    }

    @Test
    void testSupportedKeyTypes() {
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.STRING())).isTrue();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.VARCHAR(10))).isTrue();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.BYTES())).isTrue();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.BIGINT())).isTrue();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.DATE())).isTrue();
        // converted to the column type after the engine compared the keys
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.DECIMAL(10, 2))).isFalse();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.TIMESTAMP(3))).isFalse();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()))
                .isFalse();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.TIME())).isFalse();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.CHAR(3))).isFalse();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.BINARY(3))).isFalse();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.FLOAT())).isFalse();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.DOUBLE())).isFalse();
        assertThat(MapDeltaColumns.supportsKeyType(DataTypes.ARRAY(DataTypes.INT()))).isFalse();
        assertThat(
                        MapDeltaColumns.supportsKeyType(
                                DataTypes.ROW(DataTypes.FIELD(0, "a", DataTypes.INT()))))
                .isFalse();
        assertThat(MapDeltaColumns.supportsType(DataTypes.STRING())).isFalse();
        assertThat(
                        MapDeltaColumns.supportsType(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE())))
                .isTrue();
    }

    @Test
    void testMayContainDeltasAndSchemaShortcut() {
        assertThat(MapDeltaColumns.mayContainDeltas(null)).isFalse();
        assertThat(MapDeltaColumns.mayContainDeltas(Arrays.asList("id", "m"))).isFalse();
        assertThat(MapDeltaColumns.mayContainDeltas(Arrays.asList("id", "_MAP_DELTA_m"))).isTrue();
        TableSchema schema =
                TableSchema.create(
                        0L,
                        new Schema(
                                FIELDS,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                ""));
        assertThat(MapDeltaColumns.deltaFieldIds(schema, Arrays.asList("m", "s", "_MAP_DELTA_m")))
                .containsExactly(1);
        assertThat(MapDeltaColumns.hasDeltas(schema, Arrays.asList("m", "_MAP_DELTA_m"))).isTrue();
        assertThat(MapDeltaColumns.hasDeltas(schema, Arrays.asList("s", "_MAP_DELTA_m"))).isFalse();
        assertThat(MapDeltaColumns.deltaFieldIds(schema, Arrays.asList("m", "s"))).isEmpty();
        assertThat(MapDeltaColumns.deltaFieldIds(schema, null)).isEmpty();
    }

    @Test
    void testDataFileSchemaOfMapDeltaFile() {
        for (boolean nested : Arrays.asList(false, true)) {
            Map<String, String> options = new HashMap<>();
            options.put(
                    CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), String.valueOf(nested));
            TableSchema schema =
                    TableSchema.create(
                            0L,
                            new Schema(
                                    FIELDS,
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    options,
                                    ""));
            TableSchema dataFileSchema =
                    schema.dataFileSchema(Arrays.asList("s", "m", "_MAP_DELTA_x", "_MAP_DELTA_m"));
            // the delta is stored under the column's own name and type, the marker is not stored
            assertThat(dataFileSchema.fields())
                    .containsExactly(FIELDS.get(3), FIELDS.get(1), FIELDS.get(4));
        }
    }
}
