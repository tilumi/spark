/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.debug._
import org.apache.spark.sql.execution.ui.SQLHistoryListener
import org.apache.spark.ui.SparkUI

object Benchmark_SPARK_16280 {

  private def rnd = scala.util.Random

  def main(args: Array[String]) {
    // scalastyle:off
    val sqlContext = SparkSession.builder().master("local")
      .appName("Spark-16280").config("spark.executor.heartbeatInterval", "100000s")
      .config("spark.network.timeout", "100000s").config("spark.shuffle.manager", "tungsten-sort")
      .config("spark.executor.memory", "1kb")
      .config("spark.driver.memory", "1kb")
      .config("spark.sql.codegen.maxFields", "1")
      .getOrCreate()
    val sc = sqlContext.sparkContext
    import sqlContext.implicits._
    val statistics = Seq(
      (100000, 25), (100000, 50), (100000, 100), (100000, 200)
      ,
      (1000000, 25), (1000000, 50), (1000000, 100), (1000000, 200)
//      ,
//      (10000000, 25), (10000000, 50), (10000000, 100), (10000000, 200)
    ).map((pair) => {
      val rows = pair._1
      val groups = pair._2
      val dataFileName = s"data_with_${rows}_rows_and_${groups}_groups"

      val rdd1 = if (new java.io.File(dataFileName).exists) {
        sc.sequenceFile[Int, Int](dataFileName)
      }
      else {
        val rdd = sc.makeRDD(
          Seq.tabulate(rows)((i) => (
            rnd.nextInt(rows),
            rnd.nextInt(100000) % groups)
          )
        )
        rdd.saveAsSequenceFile(dataFileName)
        rdd
      }

      val df1 = rdd1.toDF("value", "key").cache()
      df1.first()
      Seq(
        5
        ,
        10
        ,
        20
        ,
        40
      ).map((bins) => {
        println(s"bins: $bins")
        Seq(
          (rows, groups, bins),
          {

            println("=============================")
            println("histogram_numeric")
            println()
            val start = java.lang.System.currentTimeMillis()
            //        df1.groupBy("key").agg(histogram_numeric("value", bins).as("agg_val")).select("agg_val").debugCodegen()
            //                  df1.groupBy("key").agg(histogram_numeric("value", bins).as("agg_val")).select("agg_val").debug()
            df1.groupBy("key").agg(histogram_numeric("value", bins).as("agg_val")).select("agg_val")
              .collect()
            //            println(s"Total: ${(java.lang.System.currentTimeMillis() - start) * 1000}")
            java.lang.System.currentTimeMillis() - start

          }, {
            println("=============================")
            println("codegen_histogram_numeric")
            println()
            val start = java.lang.System.currentTimeMillis()
            //        df1.groupBy("key").agg(codegen_histogram_numeric("value", bins).as("agg_val")).select("agg_val").debugCodegen()
            //                  df1.groupBy("key").agg(codegen_histogram_numeric("value", bins).as("agg_val")).select("agg_val").debug()
            df1.groupBy("key").agg(codegen_histogram_numeric("value", bins).as("agg_val")).select("agg_val")
            .collect()
            //            println(s"Total: ${(java.lang.System.currentTimeMillis() - start) * 1000}")
            java.lang.System.currentTimeMillis() - start

          }, {
            println("=============================")
            println("declarative_histogram_numeric")
            println()
            val start = java.lang.System.currentTimeMillis()
            //        df1.select(declarative_histogram_numeric("value", bins)).debug()
            df1.groupBy("key").agg(declarative_histogram_numeric("value", bins).as("agg_val")).select("agg_val")
            .collect()
            java.lang.System.currentTimeMillis() - start


          }, {
            println("=============================")
            println("imperative_histogram_numeric")
            println()
            val start = java.lang.System.currentTimeMillis()
            //        df1.select(imperative_histogram_numeric("value", bins)).debug()
            df1.groupBy("key").agg(imperative_histogram_numeric("value", bins).as("agg_val")).select("agg_val")
            .collect()
            java.lang.System.currentTimeMillis() - start
          }
        )
      })
    })

    statistics.flatten.map((l) => {
      println(l.mkString(","))
    })

    // scalastyle:on

    object Tabulator {
      def format(table: Seq[Seq[Any]]): String = table match {
        case Seq() => ""
        case _ =>
          val sizes = for (row <- table) yield
            (for (cell <- row) yield if (cell == null) 0 else cell.toString.length)
          val colSizes = for (col <- sizes.transpose) yield col.max
          val rows = for (row <- table) yield formatRow(row, colSizes)
          formatRows(rowSeparator(colSizes), rows)
      }

      def formatRows(rowSeparator: String, rows: Seq[String]): String = (
        rowSeparator ::
          rows.head ::
          rowSeparator ::
          rows.tail.toList :::
          rowSeparator ::
          List()).mkString("\n")

      def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
        val cells = (for ((item, size) <- row.zip(colSizes))
          yield if (size == 0) "" else ("%" + size + "s").format(item))
        cells.mkString("|", "|", "|")
      }

      def rowSeparator(colSizes: Seq[Int]): String =
        colSizes map {
          "-" * _
        } mkString("|", "|", "|")
    }

  }
}
