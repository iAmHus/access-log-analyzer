package com.learning.analyzer.preprocessor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, regexp_extract, split, to_date, trim}

class Preprocessor(val spark: SparkSession, val inputFile: String)  {


  def preprocess(): DataFrame = {

    val rawDF = spark.read.schema("""raw_data STRING"""
    .stripMargin)
    .text(inputFile)

    val splitDF: DataFrame = extractColumnsFromRegex(rawDF)

    val intermediateDF = splitDF.withColumn("httpRequestSplit", split(col("httpRequest"), "\\s"))
                                .withColumn("logNameRemoteUserSplit", split(col("logNameRemoteUser"), "\\s"))
                                .withColumn("httpMethod", col("httpRequestSplit").getItem(0))
                                .withColumn("httpURL", col("httpRequestSplit").getItem(1))
                                .withColumn("httpVersion", col("httpRequestSplit").getItem(2))
                                .withColumn("logName", col("logNameRemoteUserSplit").getItem(0))
                                .withColumn("remoteUser", col("logNameRemoteUserSplit").getItem(1))

                                .withColumn("date", to_date(col("timeStampCol"), "dd/MMM/yyyy:HH:mm:ss Z"))

    val cleanedDF = intermediateDF.select("remoteHost", "logName", "remoteUser",
                                          "date", "httpMethod", "httpURL" , "httpVersion", "httpStatusCode",
                                          "responseBytesCaptured" )
                                  .filter("date is not null")

    cleanedDF

  }

  private def extractColumnsFromRegex(rawDF: DataFrame) = {
    val REMOTE_HOST_REGEX = "(^\\S+[\\S+\\.]{2,4}\\S+)"
    val HTTP_REQUEST_REGEX = "(?<=\\\").+?(?=\\\")"
    val TIMESTAMP_REGEX = "(?<=\\[).+?(?=\\])"
    val LOGNAME_REMOTE_USER_REGEX = "^\\S+[\\S+\\.]{2,4}\\S+(.*)\\["
    val HTTP_STATUS_CODE_REGEX = ".*\\\"\\S*\\s(\\S*)"
    val RESPONSE_BYTES_CAPTURED_REGEX = "\\d+$"

    val splitDF = rawDF.select(
      trim(regexp_extract(col("raw_data"), REMOTE_HOST_REGEX, 0))
        .alias("remoteHost"),
      trim(regexp_extract(col("raw_data"), LOGNAME_REMOTE_USER_REGEX, 1))
        .alias("logNameRemoteUser"),
      trim(regexp_extract(col("raw_data"), TIMESTAMP_REGEX, 0))
        .alias("timeStampCol"),
      trim(regexp_extract(col("raw_data"), HTTP_REQUEST_REGEX, 0))
        .alias("httpRequest"),
      trim(regexp_extract(col("raw_data"), HTTP_STATUS_CODE_REGEX, 1))
        .alias("httpStatusCode"),
      trim(regexp_extract(col("raw_data"), RESPONSE_BYTES_CAPTURED_REGEX, 0))
        .alias("responseBytesCaptured")
      )
    splitDF
  }
}
