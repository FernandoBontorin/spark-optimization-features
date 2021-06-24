package com.github.fernandobontorin.args

case class Parameters(dataframes: Seq[String] = Seq(), output: String = "")

object Parameters {
  def parse(args: Seq[String]): Parameters = {
    val parserEngine =
      new scopt.OptionParser[Parameters]("spark-optimization-features") {
        head("spark-optimization-features", "0.1.0")
        opt[Seq[String]]("dataframes")
          .action((dfs, params) => {
            params.copy(dataframes = dfs)
          })
          .text("Set dataframes csv paths")
          .required()
        opt[String]("output")
          .action((path, params) => {
            params.copy(output = path)
          })
          .text("Set generated features parquet output path")
          .required()
        help("help").text(
          "spark-submit --class com.github.fernandobontorin.Application spark-optimization-features.jar \\\n" +
            "--dataframes file1.csv,file2.csv \\\n" +
            "--output features_set \\\n"
        )
      }
    parserEngine.parse(args, Parameters()) match {
      case Some(params) => params
      case _            => throw new IllegalArgumentException()
    }
  }
}
