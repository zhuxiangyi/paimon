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

package org.apache.paimon.spark.commands

import org.apache.paimon.schema.MapDeltaColumns
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolver
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, AttributeReference, Expression, ExprId, Literal, MapConcat, MapFromArrays, MapKeys, MapValues}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._

/**
 * Map deltas written by MERGE INTO on a data evolution table, see [[MapDeltaColumns]].
 *
 * For a map column whose every SET merges entries into it (`SET m = map_concat(m, ...)`), only the
 * merged entries are written, as a map-delta file; a row that is not updated writes an empty delta,
 * so the target map is neither read nor copied. A delta does not see the current value, so it can
 * only reproduce map_concat when a duplicated key takes the last value.
 */
object MapDeltaMergeInto {

  /**
   * The update columns written as map deltas. `assignedValues(attr)` are the values the matched
   * updates assign to `attr`, leaving out assignments of `attr` to itself.
   */
  def mapDeltaColumns(
      table: FileStoreTable,
      conf: SQLConf,
      updateColumns: Seq[AttributeReference],
      assignedValues: AttributeReference => Seq[Expression]): Set[ExprId] = {
    val options = table.coreOptions()
    val mapKeyLastWin = conf
      .getConfString(SQLConf.MAP_KEY_DEDUP_POLICY.key, "EXCEPTION")
      .equalsIgnoreCase("LAST_WIN")
    if (!options.dataEvolutionMapDeltaEnabled() || !mapKeyLastWin) {
      return Set.empty
    }
    val tableFields = table.rowType().getFields.asScala
    val updatedFields =
      tableFields.filter(field => updateColumns.exists(attr => resolver(field.name(), attr.name)))
    // a map delta cannot be written together with columns stored in dedicated files
    if (
      MapDeltaColumns.writesDedicatedFiles(
        table.rowType(),
        new RowType(updatedFields.asJava),
        options)
    ) {
      return Set.empty
    }
    updateColumns
      .filter {
        attr =>
          // a delta merges keys by value equality, which not every key type supports
          updatedFields.exists(
            field =>
              resolver(field.name(), attr.name) && MapDeltaColumns.supportsType(
                field.`type`())) && {
            val values = assignedValues(attr)
            values.nonEmpty && values.forall(value => mapDelta(value, attr).isDefined)
          }
      }
      .map(_.exprId)
      .toSet
  }

  /**
   * The map delta of a value assigned to the map column `attr`: the maps merged into it by
   * `map_concat(attr, ...)`, or `None` if the value is not of that form.
   *
   * For a map whose values are structs, Spark aligns the assigned value to the column type with
   * `map_from_arrays(map_keys(v), transform(map_values(v), f))`. The keys of a map are distinct and
   * `f` converts a value of the merged type, which is the column type up to nullability, to the
   * column type, so it leaves the values of the column unchanged. Aligning the merged map is
   * therefore the same as merging the aligned delta, which is what the delta becomes.
   */
  def mapDelta(value: Expression, attr: AttributeReference): Option[Expression] =
    value match {
      case MapConcat(first +: rest) if rest.nonEmpty && isAttribute(first, attr) =>
        Some(if (rest.size == 1) rest.head else MapConcat(rest))
      case MapFromArrays(MapKeys(merged), ArrayTransform(MapValues(mergedForValues), function))
          if merged.semanticEquals(mergedForValues) && merged.isInstanceOf[MapConcat] =>
        mapDelta(merged, attr).map(
          delta => MapFromArrays(MapKeys(delta), ArrayTransform(MapValues(delta), function)))
      case _ => None
    }

  /** An empty map delta, which keeps the current value of a row. */
  def emptyMap(dataType: DataType): Expression =
    Literal(
      new ArrayBasedMapData(
        new GenericArrayData(Array.empty[Any]),
        new GenericArrayData(Array.empty[Any])),
      dataType)

  private def isAttribute(expression: Expression, attr: AttributeReference): Boolean =
    expression match {
      case reference: AttributeReference => reference.sameRef(attr)
      case _ => false
    }
}
