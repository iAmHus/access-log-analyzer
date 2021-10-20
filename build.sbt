name := "access-log-analyzer"

version := "1.0.0"

scalaVersion := "2.11.12"

logLevel := Level.Info

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.11.0" % Runtime
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % Test
  )

