package com.github.fernandobontorin.jobs

import com.github.fernandobontorin.env.SparkSessionWrapper
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import java.nio.charset.StandardCharsets
import scala.math.random

object JoinDatasetPrepare extends SparkSessionWrapper {

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val dfA = (3 to 999999 by 3).map(_.toString.getBytes(StandardCharsets.UTF_8)).toDF("document")
      .select(Seq(sha2(col("document"), 512).as("hash_doc")) ++
        (1 to 39).map(n => colRandomInt().as("dfa_c_" + n)): _*)
    val dfB = (6 to 999999 by 6).map(_.toString.getBytes(StandardCharsets.UTF_8)).toDF("document")
      .select(Seq(sha2(col("document"), 512).as("hash_doc")) ++
        (1 to 14).map(n => colRandomInt().as("dfb_c_" + n)): _*)

    dfA.write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .parquet("file:///tmp/data/join/dfA")
    dfB.write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .parquet("file:///tmp/data/join/dfB")

  }

  def colRandomInt = udf(() => {
    random * 10000
  })

}
