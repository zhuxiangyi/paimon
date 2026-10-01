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

package org.apache.paimon.reader;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link DataEvolutionRow}. */
public class DataEvolutionRowTest {

    private InternalRow row1;
    private InternalRow row2;
    private DataEvolutionRow dataEvolutionRow;

    @BeforeEach
    public void setUp() {
        row1 = mock(InternalRow.class);
        row2 = mock(InternalRow.class);

        // Schema: (from row1), (from row2), (null), (from row1)
        int[] rowOffsets = new int[] {0, 1, -1, 0};
        int[] fieldOffsets = new int[] {0, 0, -1, 1};

        dataEvolutionRow = new DataEvolutionRow(2, rowOffsets, fieldOffsets);
        when(row1.getRowKind()).thenReturn(RowKind.INSERT);
        dataEvolutionRow.setRow(0, row1);
        when(row2.getRowKind()).thenReturn(RowKind.INSERT);
        dataEvolutionRow.setRow(1, row2);
    }

    @Test
    public void testGetFieldCount() {
        assertThat(dataEvolutionRow.getFieldCount()).isEqualTo(4);
    }

    @Test
    public void testSetRowOutOfBounds() {
        assertThatThrownBy(() -> dataEvolutionRow.setRow(2, mock(InternalRow.class)))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Position 2 is out of bounds for rows size 2");
    }

    @Test
    public void testRowKind() {
        assertThat(dataEvolutionRow.getRowKind()).isEqualTo(RowKind.INSERT);
        dataEvolutionRow.setRowKind(RowKind.DELETE);
        assertThat(dataEvolutionRow.getRowKind()).isEqualTo(RowKind.DELETE);
    }

    @Test
    public void testIsNullAt() {
        // Test null from rowOffsets (field added by schema evolution)
        assertThat(dataEvolutionRow.isNullAt(2)).isTrue();

        // Test null from underlying row
        when(row1.isNullAt(0)).thenReturn(true);
        assertThat(dataEvolutionRow.isNullAt(0)).isTrue();

        // Test not null
        when(row2.isNullAt(0)).thenReturn(false);
        assertThat(dataEvolutionRow.isNullAt(1)).isFalse();
    }

    @Test
    public void testGetBoolean() {
        when(row1.getBoolean(0)).thenReturn(true);
        assertThat(dataEvolutionRow.getBoolean(0)).isTrue();
    }

    @Test
    public void testGetByte() {
        when(row1.getByte(0)).thenReturn((byte) 1);
        assertThat(dataEvolutionRow.getByte(0)).isEqualTo((byte) 1);
    }

    @Test
    public void testGetShort() {
        when(row1.getShort(0)).thenReturn((short) 2);
        assertThat(dataEvolutionRow.getShort(0)).isEqualTo((short) 2);
    }

    @Test
    public void testGetInt() {
        when(row1.getInt(0)).thenReturn(3);
        assertThat(dataEvolutionRow.getInt(0)).isEqualTo(3);
    }

    @Test
    public void testGetLong() {
        when(row2.getLong(0)).thenReturn(4L);
        assertThat(dataEvolutionRow.getLong(1)).isEqualTo(4L);
    }

    @Test
    public void testGetFloat() {
        when(row1.getFloat(1)).thenReturn(5.5f);
        assertThat(dataEvolutionRow.getFloat(3)).isEqualTo(5.5f);
    }

    @Test
    public void testGetDouble() {
        when(row2.getDouble(0)).thenReturn(6.6d);
        assertThat(dataEvolutionRow.getDouble(1)).isEqualTo(6.6d);
    }

    @Test
    public void testGetString() {
        BinaryString value = BinaryString.fromString("test");
        when(row1.getString(1)).thenReturn(value);
        assertThat(dataEvolutionRow.getString(3)).isSameAs(value);
    }

    @Test
    public void testGetDecimal() {
        Decimal value = Decimal.fromUnscaledLong(123, 5, 2);
        when(row1.getDecimal(1, 5, 2)).thenReturn(value);
        assertThat(dataEvolutionRow.getDecimal(3, 5, 2)).isSameAs(value);
    }

    @Test
    public void testGetTimestamp() {
        Timestamp value = Timestamp.fromEpochMillis(1000);
        when(row2.getTimestamp(0, 3)).thenReturn(value);
        assertThat(dataEvolutionRow.getTimestamp(1, 3)).isSameAs(value);
    }

    @Test
    public void testGetBinary() {
        byte[] value = new byte[] {1, 2, 3};
        when(row1.getBinary(1)).thenReturn(value);
        assertThat(dataEvolutionRow.getBinary(3)).isSameAs(value);
    }

    @Test
    public void testGetVariant() {
        Variant value = mock(Variant.class);
        when(row1.getVariant(1)).thenReturn(value);
        assertThat(dataEvolutionRow.getVariant(3)).isSameAs(value);
    }

    @Test
    public void testGetArray() {
        InternalArray value = mock(InternalArray.class);
        when(row2.getArray(0)).thenReturn(value);
        assertThat(dataEvolutionRow.getArray(1)).isSameAs(value);
    }

    @Test
    public void testGetMap() {
        InternalMap value = mock(InternalMap.class);
        when(row1.getMap(1)).thenReturn(value);
        assertThat(dataEvolutionRow.getMap(3)).isSameAs(value);
    }

    @Test
    public void testGetRow() {
        InternalRow value = mock(InternalRow.class);
        when(row2.getRow(0, 5)).thenReturn(value);
        assertThat(dataEvolutionRow.getRow(1, 5)).isSameAs(value);
    }

    @Test
    public void testMapDeltaMerge() {
        MapType type = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        // field 0 from the base in source 2, deltas in sources 1 (older) and 0 (newer)
        DataEvolutionRow row = new DataEvolutionRow(3, new int[] {-1}, new int[] {-1});
        row.setMapDeltas(
                new DataEvolutionRow.MapDeltaField[] {
                    new DataEvolutionRow.MapDeltaField(
                            type, 2, 0, new int[] {1, 0}, new int[] {0, 0})
                });

        GenericMap base = stringMap("a", 1, "b", 2);
        row.setRows(
                new InternalRow[] {
                    GenericRow.of(stringMap("b", 30, "d", 4)),
                    GenericRow.of(stringMap("b", 20, "c", 3)),
                    GenericRow.of(base)
                });
        assertThat(row.isNullAt(0)).isFalse();
        InternalMap merged = row.getMap(0);
        assertThat(merged.size()).isEqualTo(4);
        assertThat(merged.keyArray().getString(0).toString()).isEqualTo("a");
        assertThat(merged.valueArray().getInt(0)).isEqualTo(1);
        assertThat(merged.keyArray().getString(1).toString()).isEqualTo("b");
        assertThat(merged.valueArray().getInt(1)).isEqualTo(30);
        assertThat(merged.keyArray().getString(2).toString()).isEqualTo("c");
        assertThat(merged.keyArray().getString(3).toString()).isEqualTo("d");

        // the merge is done once per row
        assertThat(row.getMap(0)).isSameAs(merged);

        // empty deltas return the base itself, without copying it
        row.setRows(
                new InternalRow[] {
                    GenericRow.of(stringMap()), GenericRow.of(stringMap()), GenericRow.of(base)
                });
        assertThat(row.getMap(0)).isSameAs(base);

        // a NULL base or a NULL delta is NULL
        row.setRows(
                new InternalRow[] {
                    GenericRow.of(stringMap("x", 1)),
                    GenericRow.of(stringMap()),
                    GenericRow.of((Object) null)
                });
        assertThat(row.isNullAt(0)).isTrue();
        row.setRows(
                new InternalRow[] {
                    GenericRow.of(stringMap("x", 1)),
                    GenericRow.of((Object) null),
                    GenericRow.of(base)
                });
        assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    public void testMapDeltaWithoutBaseIsNull() {
        MapType type = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        DataEvolutionRow row = new DataEvolutionRow(1, new int[] {-1}, new int[] {-1});
        row.setMapDeltas(
                new DataEvolutionRow.MapDeltaField[] {
                    new DataEvolutionRow.MapDeltaField(type, -1, -1, new int[] {0}, new int[] {0})
                });
        row.setRows(new InternalRow[] {GenericRow.of(stringMap("a", 1))});
        assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    public void testMapDeltaMergesBinaryKeysByContent() {
        MapType type = DataTypes.MAP(DataTypes.BYTES(), DataTypes.INT());
        DataEvolutionRow row = new DataEvolutionRow(2, new int[] {-1}, new int[] {-1});
        row.setMapDeltas(
                new DataEvolutionRow.MapDeltaField[] {
                    new DataEvolutionRow.MapDeltaField(type, 1, 0, new int[] {0}, new int[] {0})
                });
        Map<Object, Object> base = new LinkedHashMap<>();
        base.put(new byte[] {1}, 1);
        Map<Object, Object> delta = new LinkedHashMap<>();
        delta.put(new byte[] {1}, 10);
        delta.put(new byte[] {2}, 2);
        row.setRows(
                new InternalRow[] {
                    GenericRow.of(new GenericMap(delta)), GenericRow.of(new GenericMap(base))
                });

        InternalMap merged = row.getMap(0);
        assertThat(merged.size()).isEqualTo(2);
        assertThat(merged.keyArray().getBinary(0)).containsExactly(1);
        assertThat(merged.valueArray().getInt(0)).isEqualTo(10);
        assertThat(merged.keyArray().getBinary(1)).containsExactly(2);
        assertThat(merged.valueArray().getInt(1)).isEqualTo(2);
    }

    private static GenericMap stringMap(Object... kvs) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(BinaryString.fromString((String) kvs[i]), kvs[i + 1]);
        }
        return new GenericMap(map);
    }
}
