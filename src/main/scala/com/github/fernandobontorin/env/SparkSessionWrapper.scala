package com.github.fernandobontorin.env

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val spark = SparkSession.builder().getOrCreate()
}
