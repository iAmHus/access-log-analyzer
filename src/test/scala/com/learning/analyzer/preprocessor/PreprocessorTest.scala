package com.learning.analyzer.preprocessor

import com.learning.analyzer.utils.DataFrameUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DateType
import org.scalatest.FlatSpec


class PreprocessorTest extends FlatSpec {

  "Preprocessor" should "generate DF that has proper schema" in {

    val spark = SparkSession.builder
                            .master("local")
                            .config("spark.sql.shuffle.partitions", "1")
                            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val rawDF = Seq(
      ("unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985")
      ).toDF("raw_data")

    val expectedDF = Seq(
      ("unicomp6.unicomp.net", "-", "-", "1995-07-01", "GET", "/shuttle/countdown/", "HTTP/1.0", "200", "3985")
      ).toDF("remoteHost", "logName", "remoteUser",
             "date", "httpMethod", "httpURL", "httpVersion", "httpStatusCode",
             "responseBytesCaptured").withColumn("date", col("date").cast(DateType))


    val df = new Preprocessor(spark, "dummy_value")

    val regexDF = df.extractColumnsFromRegex(rawDF)

    val finalDF = df.ignoreMissingDateCols(regexDF)

    assert(DataFrameUtils.isSchemaEqual(finalDF, expectedDF))
  }

  "Preprocessor" should "map to expected cols" in {

    val spark = SparkSession.builder
                            .master("local")
                            .config("spark.sql.shuffle.partitions", "1")
                            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val rawDF = Seq(
      ("unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985")
      ).toDF("raw_data")

    val expectedDF = Seq(
      ("unicomp6.unicomp.net", "-", "-", "1995-07-01", "GET", "/shuttle/countdown/", "HTTP/1.0", "200", "3985")
      ).toDF("remoteHost", "logName", "remoteUser",
             "date", "httpMethod", "httpURL", "httpVersion", "httpStatusCode",
             "responseBytesCaptured").withColumn("date", col("date").cast(DateType))


    val df = new Preprocessor(spark, "dummy_value")

    val regexDF = df.extractColumnsFromRegex(rawDF)

    val finalDF = df.ignoreMissingDateCols(regexDF)


    assert(DataFrameUtils.isEqual(finalDF, expectedDF))
  }

  "Preprocessor" should "generate DF that has to ignore null values" in {

    val spark = SparkSession.builder
                            .master("local")
                            .config("spark.sql.shuffle.partitions", "1")
                            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val rawDF = Seq(
      ("unicomp6.unicomp.net - -  \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985")
      ).toDF("raw_data")

    val expectedDF = Seq(
      ("unicomp6.unicomp.net", "-", "-", "1995-07-01", "GET", "/shuttle/countdown/", "HTTP/1.0", "200", "3985")
      ).toDF("remoteHost", "logName", "remoteUser",
             "date", "httpMethod", "httpURL", "httpVersion", "httpStatusCode",
             "responseBytesCaptured").withColumn("date", col("date").cast(DateType))


    val df = new Preprocessor(spark, "dummy_value")

    val regexDF = df.extractColumnsFromRegex(rawDF)

    val finalDF = df.ignoreMissingDateCols(regexDF)

    assertResult(0 , "The row with null value for date should be ignored") (finalDF.count())
  }


}
