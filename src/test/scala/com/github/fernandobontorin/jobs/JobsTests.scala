package com.github.fernandobontorin.jobs

import com.github.fernandobontorin.env.SparkSessionUnitTestWrapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class JobsTests
    extends AnyFlatSpec
    with should.Matchers
    with SparkSessionUnitTestWrapper {
  "Fraud Book Processor" should "execute" in {
    FraudBookProcessor.main(Array(
      "--dataframes",
      "src/test/resources/example.csv",
      "--output",
      "target/data/fraud_book_features"
    ))
  }

  "Aggregation Processor" should "execute" in {
    AggregationProcessor.main(Array(
      "--dataframes",
      "src/test/resources/example.csv",
      "--output",
      "target/data/fraud_agg"
    ))
  }

  "ParAggregation Processor" should "parallelize execution" in {
    ParAggregationProcessor.main(Array(
      "--dataframes",
      "src/test/resources/example.csv",
      "--output",
      "target/data/fraud_agg"
    ))
  }

}
