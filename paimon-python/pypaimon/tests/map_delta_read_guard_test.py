#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import unittest
from types import SimpleNamespace

from pypaimon.read.split_read import check_no_map_deltas
from pypaimon.read.table_read import TableRead
from pypaimon.schema.map_delta_columns import (decode_map_delta, map_delta_fields,
                                               to_physical)
from pypaimon.schema.table_schema import TableSchema
from pypaimon.schema.data_types import AtomicType, DataField, MapType

ID = DataField(0, "id", AtomicType("INT"))
M = DataField(1, "m", MapType(True, AtomicType("STRING"), AtomicType("INT")))
S = DataField(2, "s", AtomicType("STRING"))
LITERAL = DataField(3, "_MAP_DELTA_x", MapType(True, AtomicType("INT"), AtomicType("INT")))
X = DataField(4, "x", MapType(True, AtomicType("INT"), AtomicType("INT")))
SCHEMA = SimpleNamespace(fields=[ID, M, S, LITERAL, X])


def _file(*write_cols, sequence=0, first_row_id=0):
    return SimpleNamespace(
        schema_id=0, file_name="data-%s.parquet" % sequence, first_row_id=first_row_id,
        max_sequence_number=sequence, write_cols=list(write_cols) if write_cols else None)


class MapDeltaReadGuardTest(unittest.TestCase):

    def _check(self, files, read_fields):
        check_no_map_deltas(files, read_fields, lambda schema_id: SCHEMA)

    def test_rejects_reading_a_map_delta(self):
        with self.assertRaises(NotImplementedError) as context:
            self._check([_file(), _file("m", "_MAP_DELTA_m", sequence=1)], [ID, M])
        self.assertIn("'m'", str(context.exception))

    def test_other_columns_can_be_read(self):
        self._check([_file(), _file("m", "_MAP_DELTA_m", sequence=1)], [ID, S])

    def test_delta_older_than_a_whole_value_is_fine(self):
        self._check(
            [_file(), _file("m", "_MAP_DELTA_m", sequence=1), _file("m", sequence=2)], [ID, M])
        with self.assertRaises(NotImplementedError):
            self._check(
                [_file(), _file("m", sequence=1), _file("m", "_MAP_DELTA_m", sequence=2)],
                [ID, M])

    def test_row_ranges_are_independent(self):
        files = [
            _file(), _file("m", "_MAP_DELTA_m", sequence=1),
            _file(first_row_id=100), _file("m", sequence=2, first_row_id=100),
        ]
        with self.assertRaises(NotImplementedError):
            self._check(files, [ID, M])

    def test_files_without_map_deltas(self):
        self._check([_file(), _file("m", sequence=1), _file("s", "id", sequence=2)], [ID, M, S])

    def test_names_that_are_not_map_deltas(self):
        # a column literally named like a map delta, a non-map column and an unknown name
        self._check([_file(), _file("_MAP_DELTA_x", "_MAP_DELTA_s", "_MAP_DELTA_y", sequence=1)],
                    [ID, M, S, LITERAL, X])
        # a marker name whose map the write columns do not list
        self._check([_file(), _file("s", "_MAP_DELTA_m", sequence=1)], [ID, M, S])
        # a column literally named like a marker, written together with the map
        self._check([_file(), _file("x", "_MAP_DELTA_x", sequence=1)], [ID, X, LITERAL])

    def test_decode_map_delta(self):
        fields = SCHEMA.fields
        self.assertEqual(decode_map_delta(fields, "_MAP_DELTA_m"), M)
        self.assertEqual(decode_map_delta(fields, "_MAP_DELTA_x"), None)
        self.assertEqual(decode_map_delta(fields, "_MAP_DELTA_s"), None)
        self.assertEqual(decode_map_delta(fields, "_MAP_DELTA_"), None)
        self.assertEqual(decode_map_delta(fields, "m"), None)

    def test_map_delta_fields(self):
        fields = SCHEMA.fields
        self.assertEqual(map_delta_fields(fields, ["id", "m", "_MAP_DELTA_m"]), {"_MAP_DELTA_m": M})
        self.assertEqual(map_delta_fields(fields, ["id", "_MAP_DELTA_m"]), {})
        self.assertEqual(map_delta_fields(fields, ["x", "_MAP_DELTA_x"]), {})
        self.assertEqual(map_delta_fields(fields, None), {})

    def test_native_read_falls_back_on_map_deltas(self):
        def split(*write_cols):
            data_file = _file(*write_cols)
            data_file.file_name = "data.parquet"
            return SimpleNamespace(files=[_file(), data_file])

        self.assertTrue(TableRead._native_split_files_supported(split("m")))
        self.assertFalse(TableRead._native_split_files_supported(split("m", "_MAP_DELTA_m")))

    def test_to_physical(self):
        write_cols = ["id", "m", "_MAP_DELTA_x", "_MAP_DELTA_m"]
        self.assertEqual(to_physical(SCHEMA.fields, write_cols), ["id", "m", "_MAP_DELTA_x"])
        self.assertEqual(write_cols, ["id", "m", "_MAP_DELTA_x", "_MAP_DELTA_m"])
        plain = ["id", "m"]
        self.assertIs(to_physical(SCHEMA.fields, plain), plain)
        without_map = ["id", "_MAP_DELTA_m"]
        self.assertIs(to_physical(SCHEMA.fields, without_map), without_map)

    def test_data_file_fields_of_map_delta_file(self):
        schema = TableSchema(
            version=TableSchema.CURRENT_VERSION, id=0, fields=list(SCHEMA.fields),
            highest_field_id=4, partition_keys=[], primary_keys=[], options={},
            comment=None, time_millis=0)
        # the map is stored whole-typed under its own name, the marker is not stored
        self.assertEqual(
            schema.data_file_fields(["s", "m", "id", "_MAP_DELTA_m"]), [S, M, ID])


if __name__ == '__main__':
    unittest.main()
