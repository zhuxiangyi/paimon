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

"""The native (Rust) paths are not taken for a table that may store map deltas.

The Rust core takes a map delta for the whole map, so planning, row-id updates, upserts,
deletes, matching a predicate on a map and commits of such a table keep to the Python
paths. Each test enables everything a native path needs and checks that only the
map-delta option turns it off, and that the option cannot be turned off.
"""

import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan
from pypaimon.schema.map_delta_columns import may_have_map_deltas
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.write import native_commit, native_update

_NATIVE = object()


class MapDeltaNativeGuardTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.warehouse = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)
        pa_schema = pa.schema([('id', pa.int32()), ('m', pa.map_(pa.string(), pa.int32()))])
        for name, map_delta in (('with_deltas', 'true'), ('without_deltas', 'false')):
            cls.catalog.create_table('default.' + name, Schema.from_pyarrow_schema(
                pa_schema, options={
                    'row-tracking.enabled': 'true',
                    'data-evolution.enabled': 'true',
                    'data-evolution.map-delta.enabled': map_delta,
                    'write.native.enabled': 'true',
                    'commit.native.enabled': 'true',
                    'scan.native-plan.enabled': 'true',
                    'deletion-vectors.enabled': 'true',
                }), False)
        cls.with_deltas = cls.catalog.get_table('default.with_deltas')
        cls.without_deltas = cls.catalog.get_table('default.without_deltas')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.warehouse, ignore_errors=True)

    def test_may_have_map_deltas(self):
        self.assertTrue(may_have_map_deltas(self.with_deltas))
        self.assertFalse(may_have_map_deltas(self.without_deltas))

    def test_row_id_writers(self):
        with patch.object(native_update, 'native_write_available', return_value=True), \
                patch.object(native_update, 'create_native_write_table',
                             return_value=_NATIVE) as create:
            self.assertIs(native_update._native_row_id_table(self.without_deltas), _NATIVE)
            create.reset_mock()
            # update, update by row id, upsert, predicate update and delete all resolve here
            self.assertIsNone(native_update._native_row_id_table(self.with_deltas))
            self.assertIsNone(native_update.create_native_update(self.with_deltas, 'job', ['m']))
            self.assertIsNone(native_update.create_native_update_by_row_id(
                self.with_deltas, 'job', 1))
            self.assertIsNone(native_update.create_native_predicate_update(
                self.with_deltas, 'job', None))
            self.assertIsNone(native_update.create_native_delete(self.with_deltas, 'job'))
            create.assert_not_called()

    def test_predicate_row_ids(self):
        builder = PredicateBuilder(self.with_deltas.fields)
        on_id = builder.equal('id', 5)
        on_map = builder.is_null('m')
        reader = Mock(return_value=[])
        with patch('pypaimon.read.native_plan.native_split_bridge_available',
                   return_value=True), \
                patch('pypaimon.read.native_plan._prepare_native_read',
                      return_value=reader) as prepare:
            for predicate in (None, on_id, on_map):
                self.assertEqual(native_update.native_predicate_row_ids(
                    self.without_deltas, predicate, []), [])
            # only a predicate on the map reads its deltas
            for predicate in (None, on_id):
                self.assertEqual(native_update.native_predicate_row_ids(
                    self.with_deltas, predicate, []), [])
            prepare.reset_mock()
            for predicate in (on_map, builder.or_predicates([on_id, on_map])):
                self.assertIsNone(native_update.native_predicate_row_ids(
                    self.with_deltas, predicate, [Mock()]))
            prepare.assert_not_called()

    def test_commit(self):
        native_table = Mock()
        with patch.object(native_commit, '_rest_catalog_supported', return_value=True), \
                patch.object(native_commit, 'native_commit_available', return_value=True), \
                patch.object(native_commit, 'create_native_write_table',
                             return_value=native_table) as create:
            self.assertIsNotNone(native_commit.create_native_commit(self.without_deltas, 'job'))
            create.reset_mock()
            self.assertIsNone(native_commit.create_native_commit(self.with_deltas, 'job'))
            create.assert_not_called()

    def test_batch_plan(self):
        with patch('pypaimon.read.native_plan.native_runtime_available', return_value=True), \
                patch('pypaimon.read.native_plan._resolved_schema_file_io_options',
                      return_value={}):
            self.assertTrue(
                self.without_deltas.new_read_builder().new_scan()._native_plan_supported())
            self.assertFalse(
                self.with_deltas.new_read_builder().new_scan()._native_plan_supported())

    def test_streaming_plan(self):
        with patch('pypaimon.read.native_plan.native_plan',
                   side_effect=AssertionError('native plan')) as plan:
            # an incremental frame, an initial one of a deletion-vector table stays on Python
            scan = AsyncStreamingTableScan(self.with_deltas)
            self.assertIsNone(scan._try_native_plan(2, incremental_range=(1, 2)))
            plan.assert_not_called()

            # without map deltas the native planner is tried; its failure is a fallback
            scan = AsyncStreamingTableScan(self.without_deltas)
            self.assertIsNone(scan._try_native_plan(2, incremental_range=(1, 2)))
            plan.assert_called_once()

    def test_option_cannot_be_disabled(self):
        key = 'data-evolution.map-delta.enabled'
        for change in (SchemaChange.set_option(key, 'false'), SchemaChange.remove_option(key)):
            # the catalog wraps the ValueError of the schema manager
            with self.assertRaises(Exception) as context:
                self.catalog.alter_table('default.with_deltas', [change], False)
            self.assertIn("Cannot disable table option '%s'" % key, str(context.exception))
        # setting it again changes nothing
        self.catalog.alter_table(
            'default.with_deltas', [SchemaChange.set_option(key, 'true')], False)
        self.assertTrue(may_have_map_deltas(self.catalog.get_table('default.with_deltas')))

        # an existing table can enable it
        self.catalog.create_table('default.enable_later', Schema.from_pyarrow_schema(
            pa.schema([('id', pa.int32())]), options={
                'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}), False)
        self.catalog.alter_table(
            'default.enable_later', [SchemaChange.set_option(key, 'true')], False)
        self.assertTrue(may_have_map_deltas(self.catalog.get_table('default.enable_later')))

    def test_option_cannot_be_changed_dynamically(self):
        key = 'data-evolution.map-delta.enabled'
        for table, value in ((self.with_deltas, 'false'), (self.with_deltas, None),
                             (self.without_deltas, 'true')):
            with self.assertRaises(ValueError) as context:
                table.copy({key: value})
            self.assertIn('not as a dynamic option', str(context.exception))
        # the current value is no change
        self.assertTrue(may_have_map_deltas(self.with_deltas.copy({key: 'true'})))
        self.assertFalse(may_have_map_deltas(self.without_deltas.copy({key: 'false'})))

if __name__ == '__main__':
    unittest.main()
