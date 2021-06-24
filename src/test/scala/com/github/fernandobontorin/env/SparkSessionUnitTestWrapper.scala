package com.github.fernandobontorin.env

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionUnitTestWrapper {
  val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    SparkSession.builder
      .config("spark.driver.cores", "1")
      .config("spark.executor.memory", "512m")
      .config("spark.driver.host", "localhost")
      .master("local[1]")
      .appName("spark-optimization-features unit tests")
      .getOrCreate()
  }
}
