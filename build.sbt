organization := "com.github.fernandobontorin"
name := "spark-optimization-features"
maintainer := "fernandorbontorin@gmail.com"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "com.github.scopt" %% "scopt" % "4.0.1"
)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")