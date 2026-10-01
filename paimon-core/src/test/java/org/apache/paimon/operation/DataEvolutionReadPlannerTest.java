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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.operation.DataEvolutionReadPlanner.DataEvolutionReadPlan;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataEvolutionReadPlanner} (pure, no-IO layout planning). */
class DataEvolutionReadPlannerTest {

    // read type: id INT, nest ROW<a INT, b STRING>
    private static final RowType READ_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "id", DataTypes.INT()),
                            new DataField(
                                    1,
                                    "nest",
                                    DataTypes.ROW(
                                            new DataField(2, "a", DataTypes.INT()),
                                            new DataField(3, "b", DataTypes.STRING())))));

    private static RowType nest(DataField... subFields) {
        return new RowType(
                Collections.singletonList(new DataField(1, "nest", DataTypes.ROW(subFields))));
    }

    @Test
    void testTopLevelPlanningKeepsLegacyWholeFieldSelection() {
        RowType avail0 = nest(new DataField(2, "a", DataTypes.INT()));
        RowType avail1 =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(
                                        1,
                                        "nest",
                                        DataTypes.ROW(new DataField(3, "b", DataTypes.STRING())))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(READ_TYPE, Arrays.asList(avail0, avail1), false)
                        .plan();

        assertThat(plan.nested).containsOnlyNulls();
        assertThat(plan.rowOffsets).containsExactly(1, 0);
        assertThat(plan.fieldOffsets).containsExactly(0, 0);
        assertThat(plan.bunchReadFields.get(0)).containsExactly(READ_TYPE.getField(1));
        assertThat(plan.bunchReadFields.get(1)).containsExactly(READ_TYPE.getField(0));
    }

    @Test
    void testMissingNonNullFieldIsRejectedInBothPlanningModes() {
        RowType readType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "id", DataTypes.INT().notNull())));
        RowType unrelated =
                new RowType(Collections.singletonList(new DataField(1, "other", DataTypes.INT())));

        for (boolean nestedFieldEnabled : Arrays.asList(false, true)) {
            assertThatThrownBy(
                            () ->
                                    new DataEvolutionReadPlanner(
                                                    readType,
                                                    Collections.singletonList(unrelated),
                                                    nestedFieldEnabled)
                                            .plan())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("id");
        }
    }

    @Test
    void testStructSplitAcrossFilesIsComposed() {
        // bunch0 (latest) provides nest.a; bunch1 provides id + nest.b
        RowType avail0 = nest(new DataField(2, "a", DataTypes.INT()));
        RowType avail1 =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(
                                        1,
                                        "nest",
                                        DataTypes.ROW(new DataField(3, "b", DataTypes.STRING())))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(READ_TYPE, Arrays.asList(avail0, avail1), true).plan();

        // id is taken whole from bunch1
        assertThat(plan.nested[0]).isNull();
        assertThat(plan.rowOffsets[0]).isEqualTo(1);
        // nest is composed across files (a from bunch0, b from bunch1)
        assertThat(plan.rowOffsets[1]).isEqualTo(-1);
        assertThat(plan.nested[1]).isNotNull();
        // each bunch only physically reads what it provides
        assertThat(plan.bunchReadFields.get(0)).hasSize(1); // nest<a>
        assertThat(plan.bunchReadFields.get(1)).hasSize(2); // id, nest<b>
    }

    @Test
    void testStructWholeFromSingleFile() {
        // bunch0 provides the whole nest; bunch1 provides id
        RowType avail0 =
                nest(
                        new DataField(2, "a", DataTypes.INT()),
                        new DataField(3, "b", DataTypes.STRING()));
        RowType avail1 =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(READ_TYPE, Arrays.asList(avail0, avail1), true).plan();

        // nest is taken whole from bunch0, not composed
        assertThat(plan.nested[1]).isNull();
        assertThat(plan.rowOffsets[1]).isEqualTo(0);
        assertThat(plan.rowOffsets[0]).isEqualTo(1);
    }

    @Test
    void testSubFieldAbsentEverywhereStaysNullWhenNullable() {
        // only nest.a is provided anywhere; nest.b (nullable) is absent
        RowType avail0 = nest(new DataField(2, "a", DataTypes.INT()));
        RowType avail1 =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(READ_TYPE, Arrays.asList(avail0, avail1), true).plan();

        // nest still composed (only a present), no exception since b is nullable
        assertThat(plan.nested[1]).isNotNull();
        assertThat(plan.bunchReadFields.get(0)).hasSize(1);
    }

    @Test
    void testDeeperThanOneLevelSplitThrows() {
        // read type: nest ROW<sub ROW<x INT, y INT>>; x and y provided by different files
        RowType readType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "nest",
                                        DataTypes.ROW(
                                                new DataField(
                                                        4,
                                                        "sub",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        5, "x", DataTypes.INT()),
                                                                new DataField(
                                                                        6,
                                                                        "y",
                                                                        DataTypes.INT())))))));
        RowType avail0 =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "nest",
                                        DataTypes.ROW(
                                                new DataField(
                                                        4,
                                                        "sub",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        5,
                                                                        "x",
                                                                        DataTypes.INT())))))));
        RowType avail1 =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "nest",
                                        DataTypes.ROW(
                                                new DataField(
                                                        4,
                                                        "sub",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        6,
                                                                        "y",
                                                                        DataTypes.INT())))))));

        assertThatThrownBy(
                        () ->
                                new DataEvolutionReadPlanner(
                                                readType, Arrays.asList(avail0, avail1), true)
                                        .plan())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testDeepAddColumnIsNullFilledInsteadOfRejected() {
        // read type: id INT, payload ROW<inner ROW<x INT, y INT>>
        // "y" was added later by ALTER TABLE ADD COLUMN payload.inner.y, so no file has it yet.
        RowType readType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(
                                        1,
                                        "payload",
                                        DataTypes.ROW(
                                                new DataField(
                                                        2,
                                                        "inner",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        3, "x", DataTypes.INT()),
                                                                new DataField(
                                                                        4,
                                                                        "y",
                                                                        DataTypes.INT())))))));
        // bunch0 (latest): an unrelated partial update touching only the top-level "id"
        RowType avail0 =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
        // bunch1: the original file, written before the deep ADD COLUMN
        RowType avail1 =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(
                                        1,
                                        "payload",
                                        DataTypes.ROW(
                                                new DataField(
                                                        2,
                                                        "inner",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        3,
                                                                        "x",
                                                                        DataTypes.INT())))))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(readType, Arrays.asList(avail0, avail1), true).plan();

        // id comes whole from the latest partial file
        assertThat(plan.rowOffsets[0]).isEqualTo(0);
        // payload.inner is entirely provided by bunch1; the missing leaf "y" must be null-filled
        // by schema evolution rather than rejected as an unsupported deep split.
        assertThat(plan.bunchReadFields.get(1)).anySatisfy(f -> assertThat(f.id()).isEqualTo(1));
    }

    @Test
    void testProjectedAddedLeafUsesParentAsReaderAnchor() {
        // Only payload.y is projected. The old file predates y, but its payload field still has to
        // be read so schema evolution can null-fill y and the union reader keeps row cardinality.
        RowType readType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "payload",
                                        new RowType(
                                                false,
                                                Collections.singletonList(
                                                        new DataField(3, "y", DataTypes.INT()))))));
        RowType unrelatedUpdate =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
        RowType oldFile =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "payload",
                                        new RowType(
                                                false,
                                                Collections.singletonList(
                                                        new DataField(2, "x", DataTypes.INT()))))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(
                                readType, Arrays.asList(unrelatedUpdate, oldFile), true)
                        .plan();

        assertThat(plan.rowOffsets[0]).isEqualTo(-1);
        assertThat(plan.nested[0]).isNotNull();
        assertThat(plan.bunchReadFields.get(0)).isEmpty();
        assertThat(plan.bunchReadFields.get(1)).containsExactly(readType.getFields().get(0));
    }

    @Test
    void testProjectedAddedLeafUsesAllWinningSiblingProvidersAsAnchors() {
        RowType readType =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(5, "added", DataTypes.INT()))));
        RowType latestX =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(2, "x", DataTypes.INT()))));
        RowType latestZ =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(4, "z", DataTypes.INT()))));
        RowType staleX =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(2, "x", DataTypes.INT()))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(
                                readType, Arrays.asList(latestX, latestZ, staleX), true)
                        .plan();

        assertThat(plan.rowOffsets[0]).isEqualTo(-1);
        assertThat(plan.nested[0]).isNotNull();
        assertThat(plan.bunchReadFields.get(0)).containsExactly(readType.getFields().get(0));
        assertThat(plan.bunchReadFields.get(1)).containsExactly(readType.getFields().get(0));
        assertThat(plan.bunchReadFields.get(2)).isEmpty();
    }

    @Test
    void testProjectedExistingLeafUsesAllWinningSiblingProvidersAsAnchors() {
        RowType readType =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(2, "x", DataTypes.INT()))));
        RowType latestX =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(2, "x", DataTypes.INT()))));
        RowType latestZ =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(4, "z", DataTypes.INT()))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(readType, Arrays.asList(latestX, latestZ), true)
                        .plan();

        assertThat(plan.rowOffsets[0]).isEqualTo(-1);
        assertThat(plan.nested[0]).isNotNull();
        assertThat(plan.bunchReadFields.get(0)).containsExactly(readType.getFields().get(0));
        assertThat(plan.bunchReadFields.get(1)).containsExactly(readType.getFields().get(0));
    }

    @Test
    void testProjectedDeepAddedLeafUsesSiblingUnderSameParent() {
        DataField projectedSub =
                new DataField(2, "sub", rowType(new DataField(4, "added", DataTypes.INT())));
        RowType readType = rowType(new DataField(1, "payload", rowType(projectedSub)));
        RowType existingSub =
                rowType(
                        new DataField(
                                1,
                                "payload",
                                rowType(
                                        new DataField(
                                                2,
                                                "sub",
                                                rowType(
                                                        new DataField(
                                                                3,
                                                                "existing",
                                                                DataTypes.INT()))))));
        RowType otherSibling =
                rowType(
                        new DataField(
                                1,
                                "payload",
                                rowType(new DataField(5, "other", DataTypes.STRING()))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(
                                readType, Arrays.asList(existingSub, otherSibling), true)
                        .plan();

        assertThat(plan.rowOffsets[0]).isEqualTo(-1);
        assertThat(plan.nested[0]).isNotNull();
        RowType subProviderReadType = (RowType) plan.bunchReadFields.get(0).get(0).type();
        assertThat(subProviderReadType.getFields()).containsExactly(projectedSub);
        assertThat(plan.bunchReadFields.get(1)).containsExactly(readType.getFields().get(0));
    }

    // ----------------------------- map deltas -----------------------------

    private static final DataField ID = new DataField(0, "id", DataTypes.INT());
    private static final DataField M =
            new DataField(1, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
    private static final RowType MAP_READ_TYPE = new RowType(Arrays.asList(ID, M));

    @Test
    void testMapDeltasAreMergedIntoTheBase() {
        for (boolean nestedFieldEnabled : Arrays.asList(false, true)) {
            // bunches latest first: two deltas of m, then the base with id and m
            List<RowType> bunches = Arrays.asList(rowType(M), rowType(M), MAP_READ_TYPE);
            DataEvolutionReadPlan plan =
                    new DataEvolutionReadPlanner(
                                    MAP_READ_TYPE,
                                    bunches,
                                    nestedFieldEnabled,
                                    Arrays.asList(ids(1), ids(1), ids()),
                                    false)
                            .plan();

            assertThat(plan.rowOffsets).containsExactly(2, -1);
            assertThat(plan.mapDeltas[0]).isNull();
            assertThat(plan.mapDeltas[1]).isNotNull();
            assertThat(plan.bunchReadFields.get(0)).containsExactly(M);
            assertThat(plan.bunchReadFields.get(1)).containsExactly(M);
            assertThat(plan.bunchReadFields.get(2)).containsExactly(ID, M);

            InternalRow row =
                    compose(
                            plan,
                            bunches,
                            GenericRow.of(map("b", 3)),
                            GenericRow.of(map("a", 2, "b", 2)),
                            GenericRow.of(7, map("a", 1, "c", 1)));
            assertThat(row.getInt(0)).isEqualTo(7);
            assertThat(row.isNullAt(1)).isFalse();
            // the older delta applies first, the latest one wins
            assertThat(toJavaMap(row.getMap(1))).isEqualTo(javaMap("a", 2, "c", 1, "b", 3));
        }
    }

    @Test
    void testWholeFileStopsTheMapDeltaChain() {
        // bunches latest first: delta, whole, older delta, base
        List<RowType> bunches = Arrays.asList(rowType(M), rowType(M), rowType(M), MAP_READ_TYPE);
        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(
                                MAP_READ_TYPE,
                                bunches,
                                false,
                                Arrays.asList(ids(1), ids(), ids(1), ids()),
                                false)
                        .plan();

        assertThat(plan.mapDeltas[1]).isNotNull();
        // neither the older delta nor the base is read for m
        assertThat(plan.bunchReadFields.get(2)).isEmpty();
        assertThat(plan.bunchReadFields.get(3)).containsExactly(ID);

        InternalRow row =
                compose(
                        plan,
                        bunches,
                        GenericRow.of(map("b", 2)),
                        GenericRow.of(map("z", 0)),
                        GenericRow.of(map("x", 9)),
                        GenericRow.of(1, map("a", 1)));
        assertThat(toJavaMap(row.getMap(1))).isEqualTo(javaMap("z", 0, "b", 2));
    }

    @Test
    void testOlderMapDeltasAreIgnoredOnceTheColumnIsWrittenWhole() {
        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(
                                MAP_READ_TYPE,
                                Arrays.asList(rowType(M), rowType(M), MAP_READ_TYPE),
                                false,
                                Arrays.asList(ids(), ids(1), ids()),
                                false)
                        .plan();

        assertThat(plan.mapDeltas).containsOnlyNulls();
        assertThat(plan.rowOffsets).containsExactly(2, 0);
        assertThat(plan.bunchReadFields.get(1)).isEmpty();
    }

    @Test
    void testMapDeltaWithoutBaseIsNull() {
        List<RowType> bunches = Arrays.asList(rowType(M), rowType(ID));
        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(
                                MAP_READ_TYPE, bunches, false, Arrays.asList(ids(1), ids()), false)
                        .plan();

        InternalRow row = compose(plan, bunches, GenericRow.of(map("a", 1)), GenericRow.of(5));
        assertThat(row.getInt(0)).isEqualTo(5);
        assertThat(row.isNullAt(1)).isTrue();
    }

    @Test
    void testMapDeltaWithoutBaseInIncrementalRead() {
        List<RowType> bunches = Arrays.asList(rowType(M), rowType(ID));
        List<Set<Integer>> deltas = Arrays.asList(ids(1), ids());
        DataField notNullMap =
                new DataField(1, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()).notNull());
        for (DataField map : Arrays.asList(M, notNullMap)) {
            RowType readType = new RowType(Arrays.asList(ID, map));
            assertThatThrownBy(
                            () ->
                                    new DataEvolutionReadPlanner(
                                                    readType, bunches, false, deltas, true)
                                            .plan())
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Cannot read map column 'm' incrementally");
        }
        // a batch read of a NOT NULL map needs its whole value
        RowType readType = new RowType(Arrays.asList(ID, notNullMap));
        assertThatThrownBy(
                        () ->
                                new DataEvolutionReadPlanner(
                                                readType, bunches, false, deltas, false)
                                        .plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not null");
    }

    @Test
    void testMapDeltasAreNotReadWithoutTheMap() {
        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(
                                rowType(ID),
                                Arrays.asList(rowType(M), MAP_READ_TYPE),
                                false,
                                Arrays.asList(ids(1), ids()),
                                false)
                        .plan();

        assertThat(plan.mapDeltas).containsOnlyNulls();
        assertThat(plan.bunchReadFields.get(0)).isEmpty();
        assertThat(plan.bunchReadFields.get(1)).containsExactly(ID);
    }

    @Test
    void testMapDeltaNextToComposedStruct() {
        DataField nest =
                new DataField(
                        2,
                        "nest",
                        DataTypes.ROW(
                                new DataField(3, "a", DataTypes.INT()),
                                new DataField(4, "b", DataTypes.INT())));
        RowType readType = new RowType(Arrays.asList(ID, M, nest));
        // bunch0: nest.a and a delta of m, bunch1: everything
        RowType partial =
                new RowType(
                        Arrays.asList(
                                M,
                                new DataField(
                                        2,
                                        "nest",
                                        DataTypes.ROW(new DataField(3, "a", DataTypes.INT())))));
        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(
                                readType,
                                Arrays.asList(partial, readType),
                                true,
                                Arrays.asList(ids(1), ids()),
                                false)
                        .plan();

        assertThat(plan.nested[2]).isNotNull();
        assertThat(plan.mapDeltas[1]).isNotNull();
        assertThat(plan.bunchReadFields.get(1)).contains(ID, M);
    }

    private static Set<Integer> ids(Integer... ids) {
        return new HashSet<>(Arrays.asList(ids));
    }

    /**
     * Builds the row of a plan from one source row per bunch, holding the fields the bunch provides
     * in the order of its type.
     */
    private static InternalRow compose(
            DataEvolutionReadPlan plan, List<RowType> bunches, GenericRow... sources) {
        DataEvolutionRow row =
                new DataEvolutionRow(sources.length, plan.rowOffsets, plan.fieldOffsets);
        row.setMapDeltas(plan.mapDeltas);
        for (int i = 0; i < sources.length; i++) {
            List<DataField> readFields = plan.bunchReadFields.get(i);
            Object[] values = new Object[readFields.size()];
            for (int f = 0; f < values.length; f++) {
                int index = bunches.get(i).getFieldIndexByFieldId(readFields.get(f).id());
                values[f] = sources[i].getField(index);
            }
            row.setRow(i, GenericRow.of(values));
        }
        return row;
    }

    private static GenericMap map(Object... kvs) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(BinaryString.fromString((String) kvs[i]), kvs[i + 1]);
        }
        return new GenericMap(map);
    }

    private static Map<String, Integer> javaMap(Object... kvs) {
        Map<String, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put((String) kvs[i], (Integer) kvs[i + 1]);
        }
        return map;
    }

    private static Map<String, Integer> toJavaMap(InternalMap map) {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (int i = 0; i < map.size(); i++) {
            result.put(
                    map.keyArray().getString(i).toString(),
                    map.valueArray().isNullAt(i) ? null : map.valueArray().getInt(i));
        }
        return result;
    }

    private static RowType rowType(DataField field) {
        return new RowType(Collections.singletonList(field));
    }
}
