package com.github.fernandobontorin.jobs

import com.github.fernandobontorin.args.Parameters
import com.github.fernandobontorin.env.SparkSessionWrapper
import com.github.fernandobontorin.features.Processor.AGE
import com.github.fernandobontorin.features.Source.{AMOUNT, CREDIT_CARD_NUMBER}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object ParAggregationProcessor extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    val params = Parameters.parse(args)
    val df = params.dataframes.par
      .map(spark.read.option("header", "true").csv)
      .reduce(_ union _)

    Seq(
      (df.select(CREDIT_CARD_NUMBER, AMOUNT), "credit_card_number"),
      (df.select(AGE, AMOUNT), "age")
    ).par.foreach {
      case (frame, key) => {
        frame
          .groupBy(key)
          .agg(
            sum("amount").as("amount_sum"),
            min("amount").as("amount_min"),
            max("amount").as("amount_max"),
            mean("amount").as("amount_mean"),
            avg("amount").as("amount_avg")
          )
          .write
          .mode(SaveMode.Overwrite)
          .parquet(params.output + "_" + key)
      }
    }

  }

}
