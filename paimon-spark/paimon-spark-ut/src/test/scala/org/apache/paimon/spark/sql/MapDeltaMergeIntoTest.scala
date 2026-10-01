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

package org.apache.paimon.spark.sql

import org.apache.paimon.Snapshot.Operation
import org.apache.paimon.append.dataevolution.DataEvolutionNormalCompactTask
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.operation.commit.RowIdExistenceConflictException
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation
import org.apache.paimon.spark.commands.{DataEvolutionCompactMergeConflictRewriter, DataEvolutionPaimonWriter, DataEvolutionRowIdConflictCommitter, PaimonSparkWriter}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.internal.SQLConf

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * End-to-end tests for key-level data evolution of map columns via Spark `MERGE INTO`: `SET m =
 * map_concat(m, ...)` should write a map-delta file holding only the merged entries, and every read
 * must return exactly what the same statement returns on a table without the feature.
 *
 * Each test runs the same statements on `t` (map deltas enabled) and on the reference table `r`
 * (disabled), then compares both, including the order of the map keys.
 */
class MapDeltaMergeIntoTest extends PaimonSparkTestBase {

  import testImplicits._

  private val lastWin = SQLConf.MAP_KEY_DEDUP_POLICY.key -> "LAST_WIN"

  private val dataEvolutionProps =
    "'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true'"

  private def createTables(
      columns: String,
      extraProps: String = "",
      partitionedBy: String = ""): Unit = {
    val extra = if (extraProps.isEmpty) "" else s", $extraProps"
    val partition = if (partitionedBy.isEmpty) "" else s"PARTITIONED BY ($partitionedBy)"
    sql(s"""CREATE TABLE t ($columns) $partition TBLPROPERTIES ($dataEvolutionProps,
           |'data-evolution.map-delta.enabled' = 'true'$extra)""".stripMargin)
    sql(s"CREATE TABLE r ($columns) $partition TBLPROPERTIES ($dataEvolutionProps$extra)")
  }

  /** Runs a statement on both tables, `$T` stands for the table name. */
  private def onBoth(statement: String): Unit = {
    Seq("t", "r").foreach(table => sql(statement.replace("$T", table)).collect())
  }

  private def assertSameAsReference(columns: String = "*"): Unit = {
    val query = s"SELECT $columns FROM %s ORDER BY id"
    val expected = sql(query.format("r")).collect().toSeq
    checkAnswer(sql(query.format("t")), expected)
  }

  /** The map column with its keys and values in order, map equality alone ignores the order. */
  private val mapColumns = "id, m, map_keys(m), map_values(m)"

  private def writeCols(tableName: String): Seq[Seq[String]] = {
    loadTable(tableName)
      .newSnapshotReader()
      .read()
      .splits()
      .asScala
      .flatMap(_.asInstanceOf[DataSplit].dataFiles().asScala)
      .map(f => Option(f.writeCols()).map(_.asScala.toSeq).getOrElse(Seq.empty))
      .toSeq
  }

  private def assertMapDeltaWritten(tableName: String, expected: Seq[String]): Unit = {
    val cols = writeCols(tableName)
    assert(cols.contains(expected), s"expected a file with writeCols == $expected, got: $cols")
  }

  private def assertNoMapDelta(tableName: String): Unit = {
    val cols = writeCols(tableName)
    assert(!cols.exists(_.exists(_.startsWith("_MAP_DELTA_"))), s"unexpected map delta in $cols")
  }

  test("Map delta: map_concat merges entries and writes only the delta") {
    withTable("s", "t", "r") {
      withSQLConf(lastWin) {
        createTables("id INT, c STRING, m MAP<STRING, INT>")
        onBoth(
          "INSERT INTO $T VALUES (1, 'a', map('x', 1, 'y', 2)), (2, 'b', map('x', 1)), " +
            "(3, 'c', map('z', 3))")

        Seq((1, Map("y" -> 20, "n" -> 5)), (3, Map("a" -> 0)))
          .toDF("id", "d")
          .createOrReplaceTempView("s")
        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d)
                 |""".stripMargin)

        assertMapDeltaWritten("t", Seq("m", "_MAP_DELTA_m"))
        assertNoMapDelta("r")
        assertSameAsReference(mapColumns)
        checkAnswer(
          sql("SELECT id, m['x'], m['y'], m['n'] FROM t ORDER BY id"),
          Seq(Row(1, 1, 20, 5), Row(2, 1, null, null), Row(3, null, null, null)))
      }
    }
  }

  test("Map delta: NULL maps and NULL deltas follow map_concat") {
    withTable("s", "t", "r") {
      withSQLConf(lastWin) {
        createTables("id INT, m MAP<STRING, INT>")
        onBoth(
          "INSERT INTO $T VALUES (1, NULL), (2, map('x', 1)), (3, map('x', 1)), " +
            "(4, map('x', 1)), (5, NULL)")

        Seq[(Int, Map[String, Integer])](
          (1, Map("a" -> 1)),
          (2, null),
          (3, Map.empty),
          (4, Map("x" -> null)))
          .toDF("id", "d")
          .createOrReplaceTempView("s")
        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d)
                 |""".stripMargin)

        assertMapDeltaWritten("t", Seq("m", "_MAP_DELTA_m"))
        assertSameAsReference(mapColumns)
        checkAnswer(
          sql("SELECT id, m IS NULL FROM t ORDER BY id"),
          Seq(Row(1, true), Row(2, true), Row(3, false), Row(4, false), Row(5, true)))
      }
    }
  }

  test("Map delta: successive merges accumulate and compaction materializes them") {
    withTable("s", "t", "r") {
      withSQLConf(lastWin) {
        createTables("id INT, m MAP<STRING, INT>")
        onBoth("INSERT INTO $T SELECT id, map('k', id) FROM range(0, 20)")

        for (round <- 1 to 3) {
          Seq(0, round, round * 2, round * 5)
            .map(id => (id, Map("k" -> -round, s"r$round" -> round)))
            .toDF("id", "d")
            .createOrReplaceTempView("s")
          onBoth("""
                   |MERGE INTO $T USING s ON $T.id = s.id
                   |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d)
                   |""".stripMargin)
          assertSameAsReference(mapColumns)
        }
        // every round writes a delta for each base file it touches
        assert(writeCols("t").count(_ == Seq("m", "_MAP_DELTA_m")) >= 3)

        sql("CALL sys.compact(table => 't', options => 'compaction.min.file-num=2')").collect()
        assertNoMapDelta("t")
        assertSameAsReference(mapColumns)
      }
    }
  }

  test("Map delta: map_concat of several maps and computed maps") {
    withTable("s", "t", "r") {
      withSQLConf(lastWin) {
        createTables("id INT, m MAP<STRING, INT>")
        onBoth("INSERT INTO $T VALUES (1, map('a', 1)), (2, map('a', 1))")

        Seq((1, Map("b" -> 2), 10), (2, Map("a" -> 3), 20))
          .toDF("id", "d", "v")
          .createOrReplaceTempView("s")
        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d, map('v', s.v, 'a', s.v))
                 |""".stripMargin)

        assertMapDeltaWritten("t", Seq("m", "_MAP_DELTA_m"))
        assertSameAsReference(mapColumns)
      }
    }
  }

  test("Map delta: unmatched rows and other clauses write an empty delta") {
    withTable("s", "t", "r") {
      withSQLConf(lastWin) {
        createTables("id INT, c STRING, m MAP<STRING, INT>")
        onBoth(
          "INSERT INTO $T VALUES (1, 'a', map('x', 1)), (2, 'b', map('x', 2)), " +
            "(3, 'c', NULL), (4, 'd', map('x', 4))")

        Seq((1, "A", Map("y" -> 1)), (2, "B", Map("y" -> 2)), (3, "C", Map("y" -> 3)))
          .toDF("id", "c", "d")
          .createOrReplaceTempView("s")
        // clauses are written out of schema order on purpose
        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED AND s.id = 1 THEN UPDATE SET m = map_concat($T.m, s.d), c = s.c
                 |WHEN MATCHED AND s.id = 2 THEN UPDATE SET c = s.c
                 |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d)
                 |""".stripMargin)

        assertMapDeltaWritten("t", Seq("c", "m", "_MAP_DELTA_m"))
        assertSameAsReference(s"$mapColumns, c")
      }
    }
  }

  test("Map delta: NOT MATCHED inserts and matched deletes") {
    withTable("s", "t", "r") {
      withSQLConf(lastWin) {
        createTables("id INT, m MAP<STRING, INT>", "'deletion-vectors.enabled' = 'true'")
        onBoth("INSERT INTO $T VALUES (1, map('x', 1)), (2, map('x', 2)), (3, map('x', 3))")

        Seq((1, Map("y" -> 1)), (2, Map("y" -> 2)), (9, Map("y" -> 9)))
          .toDF("id", "d")
          .createOrReplaceTempView("s")
        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED AND s.id = 2 THEN DELETE
                 |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d)
                 |WHEN NOT MATCHED THEN INSERT (id, m) VALUES (s.id, s.d)
                 |""".stripMargin)

        assertMapDeltaWritten("t", Seq("m", "_MAP_DELTA_m"))
        assertSameAsReference(mapColumns)
        checkAnswer(sql("SELECT id FROM t ORDER BY id"), Seq(Row(1), Row(3), Row(9)))
      }
    }
  }

  test("Map delta: self merge on _ROW_ID") {
    withTable("t", "r") {
      withSQLConf(lastWin) {
        createTables("id INT, m MAP<STRING, INT>")
        onBoth("INSERT INTO $T VALUES (1, map('x', 1)), (2, map('x', 2)), (3, NULL)")

        onBoth("""
                 |MERGE INTO $T USING $T AS s ON $T._ROW_ID = s._ROW_ID
                 |WHEN MATCHED AND s.id > 1 THEN UPDATE SET m = map_concat($T.m, map('id', s.id))
                 |""".stripMargin)

        assertMapDeltaWritten("t", Seq("m", "_MAP_DELTA_m"))
        assertSameAsReference(mapColumns)
      }
    }
  }

  test("Map delta: partitioned table and map values of complex types") {
    withTable("s", "t", "r") {
      withSQLConf(lastWin) {
        createTables(
          "id INT, m MAP<STRING, STRUCT<a: INT, b: ARRAY<STRING>>>, p STRING",
          partitionedBy = "p")
        onBoth(
          "INSERT INTO $T VALUES " +
            "(1, map('x', named_struct('a', 1, 'b', array('u'))), 'p1'), " +
            "(2, map('x', named_struct('a', 2, 'b', array('v'))), 'p2')")

        sql(
          """
            |SELECT 1 AS id, map('x', named_struct('a', 10, 'b', array('w'))) AS d
            |UNION ALL
            |SELECT 2 AS id, map('y', named_struct('a', 20, 'b', CAST(NULL AS ARRAY<STRING>))) AS d
            |""".stripMargin).createOrReplaceTempView("s")
        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d)
                 |""".stripMargin)

        assertMapDeltaWritten("t", Seq("m", "_MAP_DELTA_m"))
        assertSameAsReference(s"$mapColumns, p")
      }
    }
  }

  test("Map delta: a whole map write replaces the deltas before it") {
    withTable("s", "t", "r") {
      withSQLConf(lastWin) {
        createTables("id INT, m MAP<STRING, INT>")
        onBoth("INSERT INTO $T VALUES (1, map('x', 1)), (2, map('x', 2))")

        Seq((1, Map("y" -> 1)), (2, Map("y" -> 2))).toDF("id", "d").createOrReplaceTempView("s")
        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d)
                 |""".stripMargin)
        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED AND s.id = 1 THEN UPDATE SET m = s.d
                 |""".stripMargin)
        assertSameAsReference(mapColumns)

        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, map('z', 0))
                 |""".stripMargin)
        assertSameAsReference(mapColumns)
      }
    }
  }

  test("Map delta: falls back to a whole column write when it cannot reproduce the update") {
    withTable("s", "t", "r") {
      createTables("id INT, m MAP<STRING, INT>")
      onBoth("INSERT INTO $T VALUES (1, map('x', 1)), (2, map('x', 2))")
      Seq((1, Map("y" -> 1)), (2, Map("x" -> 20)))
        .toDF("id", "d")
        .createOrReplaceTempView("s")

      def assertWholeColumnWrite(statement: String): Unit = {
        onBoth(statement)
        assertNoMapDelta("t")
        assertSameAsReference(mapColumns)
      }

      withSQLConf(lastWin) {
        // not a merge into the column itself
        assertWholeColumnWrite("""
                                 |MERGE INTO $T USING s ON $T.id = s.id
                                 |WHEN MATCHED THEN UPDATE SET m = s.d
                                 |""".stripMargin)
        // the column is not the first map, its entries do not win
        assertWholeColumnWrite("""
                                 |MERGE INTO $T USING s ON $T.id = s.id
                                 |WHEN MATCHED THEN UPDATE SET m = map_concat(s.d, $T.m)
                                 |""".stripMargin)
        // one clause replaces the value
        assertWholeColumnWrite(
          """
            |MERGE INTO $T USING s ON $T.id = s.id
            |WHEN MATCHED AND s.id = 1 THEN UPDATE SET m = map_concat($T.m, s.d)
            |WHEN MATCHED THEN UPDATE SET m = map('only', 1)
            |""".stripMargin)
      }

      // map_concat fails on a duplicated key by default, which a delta cannot detect
      assert(spark.conf.get(SQLConf.MAP_KEY_DEDUP_POLICY.key).equalsIgnoreCase("EXCEPTION"))
      Seq((1, Map("z" -> 1))).toDF("id", "d").createOrReplaceTempView("s")
      assertWholeColumnWrite("""
                               |MERGE INTO $T USING s ON $T.id = s.id
                               |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d)
                               |""".stripMargin)
      // row 2 holds {only -> 1} after the statements above
      Seq((2, Map("only" -> 1))).toDF("id", "d").createOrReplaceTempView("s")
      Seq("t", "r").foreach {
        table =>
          val error = intercept[Exception] {
            sql(s"""
                   |MERGE INTO $table USING s ON $table.id = s.id
                   |WHEN MATCHED THEN UPDATE SET m = map_concat($table.m, s.d)
                   |""".stripMargin).collect()
          }
          assert(
            error.getMessage.contains("Duplicate map key") ||
              Option(error.getCause).exists(_.getMessage.contains("Duplicate map key")),
            error.getMessage)
      }
      assertSameAsReference(mapColumns)
    }
  }

  test("Map delta: maps with floating point keys are written whole") {
    withTable("s", "t", "r") {
      withSQLConf(lastWin) {
        createTables("id INT, m MAP<DOUBLE, STRING>")
        onBoth("INSERT INTO $T VALUES (1, map(CAST(-0.0 AS DOUBLE), 'a')), (2, map(1.5D, 'b'))")
        sql("SELECT 1 AS id, map(0.0D, 'z') AS d UNION ALL SELECT 2, map(2.5D, 'c')")
          .createOrReplaceTempView("s")
        onBoth("""
                 |MERGE INTO $T USING s ON $T.id = s.id
                 |WHEN MATCHED THEN UPDATE SET m = map_concat($T.m, s.d)
                 |""".stripMargin)

        assertNoMapDelta("t")
        assertSameAsReference(mapColumns)
      }
    }
  }

  test("Map delta: disabled by default") {
    withTable("s", "t") {
      withSQLConf(lastWin) {
        sql(s"CREATE TABLE t (id INT, m MAP<STRING, INT>) TBLPROPERTIES ($dataEvolutionProps)")
        sql("INSERT INTO t VALUES (1, map('x', 1))")
        Seq((1, Map("y" -> 1))).toDF("id", "d").createOrReplaceTempView("s")
        sql("""
              |MERGE INTO t USING s ON t.id = s.id
              |WHEN MATCHED THEN UPDATE SET m = map_concat(t.m, s.d)
              |""".stripMargin).collect()

        assertNoMapDelta("t")
        checkAnswer(sql("SELECT m['x'], m['y'] FROM t"), Seq(Row(1, 1)))
      }
    }
  }

  test("Map delta: a map column updated with a BLOB column is written whole") {
    withTable("s", "t") {
      withSQLConf(lastWin) {
        sql(s"""
               |CREATE TABLE t (id INT, m MAP<STRING, INT>, b BINARY) TBLPROPERTIES (
               |  $dataEvolutionProps,
               |  'data-evolution.map-delta.enabled' = 'true',
               |  'blob-field' = 'b')
               |""".stripMargin)
        sql("INSERT INTO t VALUES (1, map('x', 1), X'01')")
        Seq((1, Map("y" -> 1), Array[Byte](2))).toDF("id", "d", "nb").createOrReplaceTempView("s")
        sql("""
              |MERGE INTO t USING s ON t.id = s.id
              |WHEN MATCHED THEN UPDATE SET m = map_concat(t.m, s.d), b = s.nb
              |""".stripMargin).collect()

        assertNoMapDelta("t")
        checkAnswer(sql("SELECT m['x'], m['y'] FROM t"), Seq(Row(1, 1)))
      }
    }
  }

  test("Map delta: a staged map delta is not rebased after a concurrent compaction") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE t (id INT, m MAP<STRING, INT>) TBLPROPERTIES (
             |  $dataEvolutionProps,
             |  'data-evolution.map-delta.enabled' = 'true',
             |  'compaction.min.file-num' = '2',
             |  'commit.max-retries' = '0',
             |  'data-evolution.row-id-conflict-rewrite.max-size' = '0 B')
             |""".stripMargin)
      sql("INSERT INTO t VALUES (1, map('x', 1))")
      sql("INSERT INTO t VALUES (2, map('x', 2))")

      val table = loadTable("t")
      val readSnapshot = table.latestSnapshot().get()
      val dataSplits = table
        .newSnapshotReader()
        .withSnapshot(readSnapshot)
        .read()
        .splits()
        .asScala
        .collect { case split: DataSplit => split }
        .toSeq
      val firstRowIds = dataSplits.flatMap(_.dataFiles().asScala).map(_.firstRowId().longValue())
      val firstRowId = udf((rowId: Long) => firstRowIds.sorted.takeWhile(_ <= rowId).last)
      val stagedRows = sql("SELECT map('y', 1) AS m, _ROW_ID FROM t WHERE id = 1")
        .withColumn("_FIRST_ROW_ID", firstRowId(col("_ROW_ID")))
        .select("m", "_FIRST_ROW_ID", "_ROW_ID")
      val stagedDelta = DataEvolutionPaimonWriter(table, dataSplits).writePartialFields(
        stagedRows,
        table.rowType().project("m"),
        Map.empty,
        Seq("m"))

      // the compaction moves the row-id boundaries the staged delta was written against
      sql("CALL sys.compact(table => 't')").collect()

      val relation = PaimonRelation.getPaimonRelation(spark.table("t").queryExecution.analyzed)
      val error = intercept[RuntimeException] {
        DataEvolutionRowIdConflictCommitter.commit(
          spark,
          table,
          relation,
          PaimonSparkWriter(table),
          stagedDelta,
          Nil,
          readSnapshot.id(),
          Operation.MERGE)
      }
      // declined by the rewriter, the commit fails on the moved row ids
      assert(
        Iterator
          .iterate[Throwable](error)(_.getCause)
          .takeWhile(_ != null)
          .exists(_.isInstanceOf[RowIdExistenceConflictException]),
        error)
      checkAnswer(
        sql("SELECT id, m FROM t ORDER BY id"),
        Seq(Row(1, Map("x" -> 1)), Row(2, Map("x" -> 2))))
    }
  }

  test("Map delta: a compaction is not rebased onto a map delta") {
    withTable("s", "t") {
      withSQLConf(lastWin) {
        sql(s"""
               |CREATE TABLE t (id INT, m MAP<STRING, INT>) TBLPROPERTIES (
               |  $dataEvolutionProps,
               |  'data-evolution.map-delta.enabled' = 'true',
               |  'bucket' = '-1',
               |  'compaction.min.file-num' = '2')
               |""".stripMargin)
        sql("""
              |INSERT INTO t
              |SELECT /*+ REPARTITION(1) */ id, map('x', id) FROM VALUES (1), (2) AS S(id)
              |""".stripMargin)
        val table = loadTable("t")
        val relation =
          PaimonRelation.getPaimonRelation(spark.table("t").queryExecution.analyzed)

        def mergeDelta(key: String): Unit = {
          Seq((1, Map(key -> 1)), (2, Map(key -> 2))).toDF("id", "d").createOrReplaceTempView("s")
          sql("""
                |MERGE INTO t USING s ON t.id = s.id
                |WHEN MATCHED THEN UPDATE SET m = map_concat(t.m, s.d)
                |""".stripMargin).collect()
        }

        // a compaction is staged over the base and a first delta
        mergeDelta("a")
        val files = table
          .newSnapshotReader()
          .read()
          .dataSplits()
          .asScala
          .flatMap(_.dataFiles().asScala)
          .sortBy(_.maxSequenceNumber())
        assert(files.size == 2)
        val stagedMessage = new DataEvolutionNormalCompactTask(BinaryRow.EMPTY_ROW, files.asJava)
          .doCompact(table, "staged-compact")
        val baseSnapshot = table.latestSnapshot().get()

        // a second delta lands before the compaction commits
        mergeDelta("b")
        val expected = sql("SELECT id, m FROM t ORDER BY id").collect().toSeq
        assert(
          expected == Seq(
            Row(1, Map("x" -> 1, "a" -> 1, "b" -> 1)),
            Row(2, Map("x" -> 2, "a" -> 2, "b" -> 2))))

        val rewritten = new DataEvolutionCompactMergeConflictRewriter(table, relation)
          .rewrite(
            spark,
            baseSnapshot,
            table.latestSnapshot().get(),
            Collections.singletonList(stagedMessage))
        assert(!rewritten.isPresent)
        checkAnswer(sql("SELECT id, m FROM t ORDER BY id"), expected)
      }
    }
  }
}
