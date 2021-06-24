package com.github.fernandobontorin.args

import com.github.fernandobontorin.env.SparkSessionUnitTestWrapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ParametersTests
    extends AnyFlatSpec
    with should.Matchers
    with SparkSessionUnitTestWrapper {

  "Parameters" should "parser and extract arguments to object" in {
    val args = Array(
      "--dataframes",
      "input.csv,input2.csv,input3.csv",
      "--output",
      "output.parquet"
    )
    val params = Parameters.parse(args)
    args(1).split(",").head should be(params.dataframes.head)
    args(3) should be(params.output)
  }

  it should "store arguments" in {
    val params = Parameters(Seq("input"), "output")
    params.dataframes.head should be("input")
    params.output should be("output")
  }

  it should "has default empty state" in {
    val params = Parameters()
    params.dataframes.isEmpty should be(true)
    params.output should be("")
  }

  it should "throw illegal argument exception on missing dataframes args" in {
    val args =
      Array("input.csv,input2.csv,input3.csv", "--output", "output.parquet")
    assertThrows[IllegalArgumentException] {
      val params = Parameters.parse(args)
    }
  }

  it should "throw illegal argument exception on missing output args" in {
    val args =
      Array("--dataframes", "input.csv,input2.csv,input3.csv", "output.parquet")
    assertThrows[IllegalArgumentException] {
      val params = Parameters.parse(args)
    }
  }

}
