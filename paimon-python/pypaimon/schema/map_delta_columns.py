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

"""Map-delta write columns of data evolution files.

With 'data-evolution.map-delta.enabled', a file may store a top-level map column ``m`` as
map deltas: each value only holds the entries to merge into the current value of the row.
Such a file lists ``m`` in its write columns like any other column and appends a marker
``_MAP_DELTA_m``, which is not a physical column, e.g. ``[m, d, _MAP_DELTA_m]``. An entry is
a marker only if it does not name a field itself, the rest names a map field, and the write
columns also list that field, so ordinary columns keep their meaning.
"""

from typing import Dict, Iterable, List, Optional

from pypaimon.schema.data_types import DataField, MapType

MAP_DELTA_PREFIX = "_MAP_DELTA_"


def _marker_field(by_name: Dict[str, DataField], write_col: str) -> Optional[DataField]:
    if not write_col.startswith(MAP_DELTA_PREFIX) or write_col in by_name:
        return None
    field = by_name.get(write_col[len(MAP_DELTA_PREFIX):])
    return field if field is not None and isinstance(field.type, MapType) else None


def map_delta_fields(fields: Iterable[DataField],
                     write_cols: Optional[List[str]]) -> Dict[str, DataField]:
    """The map field of each marker in ``write_cols``, by marker."""
    if not write_cols or not any(col.startswith(MAP_DELTA_PREFIX) for col in write_cols):
        return {}
    by_name = {field.name: field for field in fields}
    result = {}
    for write_col in write_cols:
        field = _marker_field(by_name, write_col)
        if field is not None and field.name in write_cols:
            result[write_col] = field
    return result


def decode_map_delta(fields: Iterable[DataField], write_col: str) -> Optional[DataField]:
    """The map field whose marker ``write_col`` is, by name only, or None if it is not one."""
    return _marker_field({field.name: field for field in fields}, write_col)


def to_physical(fields: Iterable[DataField], write_cols: List[str]) -> List[str]:
    """The physical columns of ``write_cols``, without the markers of map deltas."""
    markers = map_delta_fields(fields, write_cols)
    if not markers:
        return write_cols
    return [col for col in write_cols if col not in markers]
