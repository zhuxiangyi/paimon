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

import org.apache.paimon.data.shredding.MapSelectedKeysMetadataUtils;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Pure (no-IO) planner for data evolution reads. Given the requested read row type and, for each
 * column-group file ("bunch"), the row type it physically provides (its written columns, already
 * wrapped with row-tracking fields), it produces the source offsets and per-bunch physical read
 * fields.
 *
 * <p>With nested-field evolution disabled, fields are matched only by top-level id. With it
 * enabled, the planner selects the latest provider per leaf and composes nested fields split across
 * bunches. Only one level of nested composition is supported; deeper or cross-file splits of a
 * sub-struct throw {@link UnsupportedOperationException}.
 *
 * <p>A map column whose latest providers are map-delta files (see {@link
 * org.apache.paimon.schema.MapDeltaColumns}) is merged from all of those deltas and the newest file
 * storing the column whole, independently of the nested-field mode.
 *
 * <p>Separating this from {@link DataEvolutionSplitRead} keeps schema resolution and reader
 * creation out of the layout logic and lets both planning modes be unit-tested directly.
 */
class DataEvolutionReadPlanner {

    private final RowType readRowType;
    // for each bunch, the (row-tracked) row type it physically provides
    private final List<RowType> bunchAvailTypes;
    private final boolean nestedFieldEnabled;
    // for each bunch, the ids of the map fields it stores as map deltas
    private final List<Set<Integer>> bunchMapDeltaFieldIds;
    // whether the bunches are only the files added in a range of snapshots
    private final boolean incremental;

    DataEvolutionReadPlanner(
            RowType readRowType, List<RowType> bunchAvailTypes, boolean nestedFieldEnabled) {
        this(readRowType, bunchAvailTypes, nestedFieldEnabled, null, false);
    }

    DataEvolutionReadPlanner(
            RowType readRowType,
            List<RowType> bunchAvailTypes,
            boolean nestedFieldEnabled,
            @Nullable List<Set<Integer>> bunchMapDeltaFieldIds,
            boolean incremental) {
        this.readRowType = readRowType;
        this.bunchAvailTypes = bunchAvailTypes;
        this.nestedFieldEnabled = nestedFieldEnabled;
        if (bunchMapDeltaFieldIds == null) {
            bunchMapDeltaFieldIds = new ArrayList<>(bunchAvailTypes.size());
            for (int i = 0; i < bunchAvailTypes.size(); i++) {
                bunchMapDeltaFieldIds.add(Collections.emptySet());
            }
        }
        checkArgument(
                bunchMapDeltaFieldIds.size() == bunchAvailTypes.size(),
                "Map delta field ids must be given for every bunch.");
        this.bunchMapDeltaFieldIds = bunchMapDeltaFieldIds;
        this.incremental = incremental;
    }

    DataEvolutionReadPlan plan() {
        DataEvolutionReadPlan plan = nestedFieldEnabled ? planNested() : planTopLevel();
        planMapDeltas(plan);
        List<DataField> readFields = readRowType.getFields();
        for (int i = 0; i < readFields.size(); i++) {
            if (plan.rowOffsets[i] == -1 && plan.nested[i] == null && plan.mapDeltas[i] == null) {
                checkArgument(
                        readFields.get(i).type().isNullable(),
                        "Field %s is not null but can't find any file contains it.",
                        readFields.get(i));
            }
        }
        return plan;
    }

    private DataEvolutionReadPlan planTopLevel() {
        List<DataField> allReadFields = readRowType.getFields();
        int numFields = allReadFields.size();
        int[] readFieldIds = allReadFields.stream().mapToInt(DataField::id).toArray();
        int[] rowOffsets = new int[numFields];
        int[] fieldOffsets = new int[numFields];
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);

        List<List<DataField>> bunchReadFields = new ArrayList<>();
        for (int i = 0; i < bunchAvailTypes.size(); i++) {
            Set<Integer> availableFieldIds =
                    bunchAvailTypes.get(i).getFields().stream()
                            .map(DataField::id)
                            .collect(Collectors.toSet());
            List<DataField> readFields = new ArrayList<>();
            for (int j = 0; j < readFieldIds.length; j++) {
                if (rowOffsets[j] == -1 && availableFieldIds.contains(readFieldIds[j])) {
                    rowOffsets[j] = i;
                    fieldOffsets[j] = readFields.size();
                    readFields.add(allReadFields.get(j));
                }
            }
            bunchReadFields.add(readFields);
        }

        return new DataEvolutionReadPlan(
                rowOffsets,
                fieldOffsets,
                new DataEvolutionRow.NestedField[numFields],
                bunchReadFields);
    }

    private DataEvolutionReadPlan planNested() {
        List<DataField> allReadFields = readRowType.getFields();
        int numFields = allReadFields.size();
        int numBunches = bunchAvailTypes.size();

        // gather, per bunch, the set of leaf field ids it physically provides
        List<Set<Integer>> bunchLeaves = new ArrayList<>();
        for (int i = 0; i < numBunches; i++) {
            Set<Integer> leaves = new HashSet<>();
            collectLeafIds(bunchAvailTypes.get(i).getFields(), leaves);
            bunchLeaves.add(leaves);
        }

        // decide, per read field, whether it is taken whole from one file or composed from several
        // files at sub-field granularity. Files are already sorted latest-first, so the first bunch
        // providing a leaf wins (latest-wins semantics, now at sub-field level).
        // selection per bunch: topFieldId -> null (whole) or set of selected sub-field ids
        List<Map<Integer, Set<Integer>>> bunchSelection = new ArrayList<>();
        for (int i = 0; i < numBunches; i++) {
            bunchSelection.add(new LinkedHashMap<>());
        }

        int[] rowOffsets = new int[numFields];
        int[] fieldOffsets = new int[numFields];
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);
        DataEvolutionRow.NestedField[] nested = new DataEvolutionRow.NestedField[numFields];
        boolean[] composite = new boolean[numFields];
        int[] wholeBunch = new int[numFields];
        Arrays.fill(wholeBunch, -1);
        List<Set<Integer>> nullnessAnchors = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            nullnessAnchors.add(new LinkedHashSet<>());
        }

        for (int j = 0; j < numFields; j++) {
            DataField rf = allReadFields.get(j);
            // a read of selected map keys is the map, the fields of its ROW type are no table
            // fields
            boolean wholeField =
                    !(rf.type() instanceof RowType)
                            || MapSelectedKeysMetadataUtils.isMapSelectedKeysField(rf);
            List<Integer> leaves = wholeField ? Collections.singletonList(rf.id()) : leafIdsOf(rf);
            Map<Integer, Integer> leafProvider = new HashMap<>();
            Set<Integer> providers = new LinkedHashSet<>();
            for (int leaf : leaves) {
                int p = providerOf(leaf, bunchLeaves);
                if (p >= 0) {
                    leafProvider.put(leaf, p);
                    providers.add(p);
                }
            }

            if (wholeField) {
                if (!providers.isEmpty()) {
                    int b = providers.iterator().next();
                    bunchSelection.get(b).put(rf.id(), null);
                    wholeBunch[j] = b;
                }
                continue;
            }

            // A ROW's nullness is determined by all of its latest sibling providers, including
            // siblings omitted by the projection. Otherwise projecting only nest.a could return a
            // null nest from a later nest.a file even though nest.b in another winning file keeps
            // the merged nest non-null.
            Set<Integer> parentProviders =
                    topFieldProvidersOf(rf.id(), bunchAvailTypes, bunchLeaves);
            if (parentProviders.isEmpty()) {
                // The whole top-level ROW is absent. Leave it unplanned so the caller can either
                // null-fill a nullable field or reject a missing non-null field.
                continue;
            }
            boolean allLeavesCovered = leafProvider.size() == leaves.size();
            if (providers.size() == 1 && allLeavesCovered && parentProviders.equals(providers)) {
                int b = providers.iterator().next();
                bunchSelection.get(b).put(rf.id(), null);
                wholeBunch[j] = b;
            } else {
                composite[j] = true;
                nullnessAnchors.get(j).addAll(parentProviders);
                for (DataField sub : ((RowType) rf.type()).getFields()) {
                    Set<Integer> subProviders = new LinkedHashSet<>();
                    for (int leaf : leafIdsOf(sub)) {
                        int p = leafProvider.getOrDefault(leaf, -1);
                        if (p >= 0) {
                            subProviders.add(p);
                        }
                    }
                    if (subProviders.isEmpty()) {
                        // Every requested leaf may have been added after the files were written.
                        // Find the latest provider of older siblings under this direct sub-field;
                        // reading that sub-field preserves its ROW nullness while schema evolution
                        // null-fills the requested leaves.
                        subProviders =
                                subFieldProvidersOf(
                                        rf.id(), sub.id(), bunchAvailTypes, bunchLeaves);
                    }
                    if (subProviders.size() > 1) {
                        throw new UnsupportedOperationException(
                                "Sub-field-level data evolution does not yet support splitting a "
                                        + "nested sub-field ("
                                        + rf.name()
                                        + "."
                                        + sub.name()
                                        + ") across multiple files.");
                    }
                    if (subProviders.size() == 1) {
                        // The single provider may hold only part of this nested sub-struct, but
                        // the leaves it lacks are then absent from EVERY bunch (a cross-provider
                        // deep split is already rejected above). That is the normal shape after a
                        // deep ADD COLUMN, so read the sub-field whole from its provider and let
                        // the schema-evolution mapping null-fill the leaves the file predates.
                        int b = subProviders.iterator().next();
                        bunchSelection
                                .get(b)
                                .computeIfAbsent(rf.id(), k -> new LinkedHashSet<>())
                                .add(sub.id());
                    }
                    // else: sub-field absent everywhere -> stays null
                }
                for (int parentProvider : parentProviders) {
                    Map<Integer, Set<Integer>> selection = bunchSelection.get(parentProvider);
                    if (!selection.containsKey(rf.id())) {
                        // This provider only contributes an unprojected sibling. Read the projected
                        // shape as a hidden anchor so it still participates in parent nullness.
                        selection.put(rf.id(), null);
                    }
                }
            }
        }

        // materialize each bunch's partial read row type and the offset maps.
        List<List<DataField>> bunchReadFields = new ArrayList<>();
        List<Map<Integer, Integer>> bunchTopOffset = new ArrayList<>();
        List<Map<Integer, Map<Integer, Integer>>> bunchSubOffset = new ArrayList<>();
        for (int i = 0; i < numBunches; i++) {
            Map<Integer, Set<Integer>> sel = bunchSelection.get(i);
            List<DataField> readFields = new ArrayList<>();
            Map<Integer, Integer> topOffset = new HashMap<>();
            Map<Integer, Map<Integer, Integer>> subOffset = new HashMap<>();
            for (Map.Entry<Integer, Set<Integer>> e : sel.entrySet()) {
                int topId = e.getKey();
                Set<Integer> subs = e.getValue();
                DataField readTop = readRowType.getField(topId);
                if (subs == null) {
                    readFields.add(readTop);
                    topOffset.put(topId, readFields.size() - 1);
                } else {
                    RowType readStruct = (RowType) readTop.type();
                    List<DataField> chosen = new ArrayList<>();
                    Map<Integer, Integer> subToIdx = new HashMap<>();
                    for (DataField s : readStruct.getFields()) {
                        if (subs.contains(s.id())) {
                            subToIdx.put(s.id(), chosen.size());
                            chosen.add(s);
                        }
                    }
                    RowType partial = new RowType(readStruct.isNullable(), chosen);
                    readFields.add(readTop.newType(partial));
                    topOffset.put(topId, readFields.size() - 1);
                    subOffset.put(topId, subToIdx);
                }
            }
            bunchReadFields.add(readFields);
            bunchTopOffset.add(topOffset);
            bunchSubOffset.add(subOffset);
        }

        // wire output offsets (whole fields) and nested composition plans (split structs).
        for (int j = 0; j < numFields; j++) {
            DataField rf = allReadFields.get(j);
            if (composite[j]) {
                List<DataField> subFields = ((RowType) rf.type()).getFields();
                int subCount = subFields.size();
                int[] subRowOffsets = new int[subCount];
                int[] subFieldOffsets = new int[subCount];
                Arrays.fill(subRowOffsets, -1);
                Arrays.fill(subFieldOffsets, -1);
                Map<Integer, Integer> bunchToPartial = new LinkedHashMap<>();
                List<int[]> partials = new ArrayList<>();
                for (int b : nullnessAnchors.get(j)) {
                    Map<Integer, Integer> subOffsets = bunchSubOffset.get(b).get(rf.id());
                    bunchToPartial.put(b, partials.size());
                    partials.add(
                            new int[] {
                                b,
                                bunchTopOffset.get(b).get(rf.id()),
                                subOffsets == null ? subFields.size() : subOffsets.size()
                            });
                }
                for (int s = 0; s < subCount; s++) {
                    int subId = subFields.get(s).id();
                    int b = findSubProvider(rf.id(), subId, bunchSubOffset);
                    if (b < 0) {
                        // no file provides this sub-field; it stays null, so it must be nullable
                        checkArgument(
                                subFields.get(s).type().isNullable(),
                                "Sub-field %s.%s is not null but can't find any file contains it.",
                                rf.name(),
                                subFields.get(s).name());
                        continue;
                    }
                    Integer pIdx = bunchToPartial.get(b);
                    if (pIdx == null) {
                        int topOff = bunchTopOffset.get(b).get(rf.id());
                        int size = bunchSubOffset.get(b).get(rf.id()).size();
                        pIdx = partials.size();
                        bunchToPartial.put(b, pIdx);
                        partials.add(new int[] {b, topOff, size});
                    }
                    subRowOffsets[s] = pIdx;
                    subFieldOffsets[s] = bunchSubOffset.get(b).get(rf.id()).get(subId);
                }
                int p = partials.size();
                int[] pr = new int[p];
                int[] po = new int[p];
                int[] ps = new int[p];
                for (int k = 0; k < p; k++) {
                    pr[k] = partials.get(k)[0];
                    po[k] = partials.get(k)[1];
                    ps[k] = partials.get(k)[2];
                }
                nested[j] =
                        new DataEvolutionRow.NestedField(
                                pr, po, ps, subRowOffsets, subFieldOffsets);
            } else if (wholeBunch[j] >= 0) {
                int b = wholeBunch[j];
                rowOffsets[j] = b;
                fieldOffsets[j] = bunchTopOffset.get(b).get(rf.id());
            }
        }

        return new DataEvolutionReadPlan(rowOffsets, fieldOffsets, nested, bunchReadFields);
    }

    /**
     * Replaces the source of every read map field whose latest providers are map deltas with a
     * merge of those deltas and the newest bunch storing the field whole (its base). Bunches are
     * ordered latest first. A field without a base is {@code null}: it did not exist when the rows
     * were written, and a delta of a {@code null} map is {@code null}. An incremental read only
     * sees the files added in its range, so a missing base does not tell that and the read fails:
     * neither {@code null} nor the deltas are the value of the field. A read of selected map keys
     * fails as well when it meets a delta, it does not hold the whole map to merge into.
     */
    private void planMapDeltas(DataEvolutionReadPlan plan) {
        if (bunchMapDeltaFieldIds.stream().allMatch(Set::isEmpty)) {
            return;
        }
        List<DataField> readFields = readRowType.getFields();
        for (int j = 0; j < readFields.size(); j++) {
            DataField rf = readFields.get(j);
            // a read of selected map keys has the map's id but a ROW type
            boolean selectedKeys = MapSelectedKeysMetadataUtils.isMapSelectedKeysField(rf);
            if (!(rf.type() instanceof MapType) && !selectedKeys) {
                continue;
            }
            List<Integer> deltas = new ArrayList<>();
            int base = -1;
            for (int b = 0; b < bunchAvailTypes.size(); b++) {
                if (!bunchAvailTypes.get(b).containsField(rf.id())) {
                    continue;
                }
                if (bunchMapDeltaFieldIds.get(b).contains(rf.id())) {
                    deltas.add(b);
                } else {
                    base = b;
                    break;
                }
            }
            if (deltas.isEmpty()) {
                // the latest provider stores the field whole, the plan already takes it from there
                continue;
            }
            if (selectedKeys) {
                // a delta does not tell an absent key from a NULL value once read as selected keys
                throw new UnsupportedOperationException(
                        String.format(
                                "Cannot read selected keys of map column '%s': the read range "
                                        + "holds map deltas of it, which can only be merged into "
                                        + "the whole map. Read the whole map instead.",
                                rf.name()));
            }
            if (base < 0 && incremental) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Cannot read map column '%s' incrementally: the read range holds "
                                        + "map deltas of it but not its whole value. Read the "
                                        + "table in batch mode, or compact it first.",
                                rf.name()));
            }
            checkArgument(
                    base >= 0 || rf.type().isNullable(),
                    "Field %s is not null but can't find any file contains it whole.",
                    rf);
            Collections.reverse(deltas);
            int[] deltaReaders = new int[deltas.size()];
            int[] deltaOffsets = new int[deltas.size()];
            for (int k = 0; k < deltas.size(); k++) {
                deltaReaders[k] = deltas.get(k);
                deltaOffsets[k] = readOffset(plan.bunchReadFields.get(deltas.get(k)), rf);
            }
            int baseOffset = base < 0 ? -1 : readOffset(plan.bunchReadFields.get(base), rf);
            plan.mapDeltas[j] =
                    new DataEvolutionRow.MapDeltaField(
                            (MapType) rf.type(), base, baseOffset, deltaReaders, deltaOffsets);
            plan.rowOffsets[j] = -1;
            plan.fieldOffsets[j] = -1;
        }
    }

    /** The offset of {@code field} in a bunch's read fields, adding it if it is not read yet. */
    private static int readOffset(List<DataField> bunchReadFields, DataField field) {
        for (int i = 0; i < bunchReadFields.size(); i++) {
            if (bunchReadFields.get(i).id() == field.id()) {
                return i;
            }
        }
        bunchReadFields.add(field);
        return bunchReadFields.size() - 1;
    }

    /** Collect (recursively) the leaf field ids of {@code fields}; only ROW types recurse. */
    private static void collectLeafIds(List<DataField> fields, Collection<Integer> out) {
        for (DataField f : fields) {
            if (f.type() instanceof RowType) {
                collectLeafIds(((RowType) f.type()).getFields(), out);
            } else {
                out.add(f.id());
            }
        }
    }

    private static List<Integer> leafIdsOf(DataField field) {
        List<Integer> result = new ArrayList<>();
        collectLeafIds(Collections.singletonList(field), result);
        return result;
    }

    private static int providerOf(int leafId, List<Set<Integer>> bunchLeaves) {
        for (int i = 0; i < bunchLeaves.size(); i++) {
            if (bunchLeaves.get(i).contains(leafId)) {
                return i;
            }
        }
        return -1;
    }

    private static Set<Integer> topFieldProvidersOf(
            int fieldId, List<RowType> bunchTypes, List<Set<Integer>> bunchLeaves) {
        Set<Integer> siblingLeaves = new LinkedHashSet<>();
        for (RowType bunchType : bunchTypes) {
            if (bunchType.containsField(fieldId)) {
                collectLeafIds(
                        Collections.singletonList(bunchType.getField(fieldId)), siblingLeaves);
            }
        }
        Set<Integer> providers = new LinkedHashSet<>();
        for (int leaf : siblingLeaves) {
            int provider = providerOf(leaf, bunchLeaves);
            if (provider >= 0) {
                providers.add(provider);
            }
        }
        return providers;
    }

    private static Set<Integer> subFieldProvidersOf(
            int topFieldId,
            int subFieldId,
            List<RowType> bunchTypes,
            List<Set<Integer>> bunchLeaves) {
        Set<Integer> siblingLeaves = new LinkedHashSet<>();
        for (RowType bunchType : bunchTypes) {
            if (!bunchType.containsField(topFieldId)) {
                continue;
            }
            DataField topField = bunchType.getField(topFieldId);
            if (topField.type() instanceof RowType
                    && ((RowType) topField.type()).containsField(subFieldId)) {
                collectLeafIds(
                        Collections.singletonList(((RowType) topField.type()).getField(subFieldId)),
                        siblingLeaves);
            }
        }
        Set<Integer> providers = new LinkedHashSet<>();
        for (int leaf : siblingLeaves) {
            int provider = providerOf(leaf, bunchLeaves);
            if (provider >= 0) {
                providers.add(provider);
            }
        }
        return providers;
    }

    private static int findSubProvider(
            int topId, int subId, List<Map<Integer, Map<Integer, Integer>>> bunchSubOffset) {
        for (int b = 0; b < bunchSubOffset.size(); b++) {
            Map<Integer, Integer> sm = bunchSubOffset.get(b).get(topId);
            if (sm != null && sm.containsKey(subId)) {
                return b;
            }
        }
        return -1;
    }

    /**
     * Immutable result of {@link DataEvolutionReadPlanner#plan()}: how each read field is sourced
     * (whole via {@code rowOffsets}/{@code fieldOffsets}, or composed via {@code nested}) and, per
     * bunch, the (possibly partial nested) fields to physically read.
     */
    static class DataEvolutionReadPlan {

        // per read field: the bunch a whole field is taken from (-1 if absent or composed)
        final int[] rowOffsets;
        // per read field: the field offset within that bunch's read row (-1 if absent or composed)
        final int[] fieldOffsets;
        // per read field: the sub-field assembly plan for a struct split across files (null if
        // whole)
        final DataEvolutionRow.NestedField[] nested;
        // per read field: the merge plan for a map column with map deltas (null if whole)
        final DataEvolutionRow.MapDeltaField[] mapDeltas;
        // per bunch: the fields (with partial nested structs) to read from that file
        final List<List<DataField>> bunchReadFields;

        DataEvolutionReadPlan(
                int[] rowOffsets,
                int[] fieldOffsets,
                DataEvolutionRow.NestedField[] nested,
                List<List<DataField>> bunchReadFields) {
            this.rowOffsets = rowOffsets;
            this.fieldOffsets = fieldOffsets;
            this.nested = nested;
            this.mapDeltas = new DataEvolutionRow.MapDeltaField[rowOffsets.length];
            this.bunchReadFields = bunchReadFields;
        }

        boolean hasMapDeltas() {
            for (DataEvolutionRow.MapDeltaField mapDelta : mapDeltas) {
                if (mapDelta != null) {
                    return true;
                }
            }
            return false;
        }
    }
}
