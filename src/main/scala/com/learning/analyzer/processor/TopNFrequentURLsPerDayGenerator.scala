package com.learning.analyzer.processor

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, rank}

class TopNFrequentURLsPerDayGenerator(val topN: Int, val cleanedDF: DataFrame) {


  private val logger = Logger.getLogger("com.learning.analyzer.processor.TopNFrequentURLsPerDayGenerator")

  def generate(): DataFrame = {

    logger.info("Process to determine the TopNFrequentURLsPerDay started")

    val relevantDF = getRelevantData(cleanedDF)

    val urlGroupedDF = relevantDF.groupBy("date", "httpURL")
                                 .agg(count(lit(1)).alias("NumOfRecordsPerURL"))


    val specForRecordsPerURL = Window.partitionBy("date")
                                     .orderBy(col("NumOfRecordsPerURL")
                                                .desc)

    val topNFrequentURLsPerDay = urlGroupedDF.withColumn("rnk", rank().over(specForRecordsPerURL))
                                             .where(s"rnk <= $topN")
                                             .sort("date", "rnk")

    logger.info("Process to determine the TopNFrequentURLsPerDay completed")

    topNFrequentURLsPerDay
  }

  private def getRelevantData(inputDF: DataFrame): DataFrame = {

    val relevantDF = inputDF.select("date", "httpURL")
                            .filter("httpURL is not null")

    logger.info("Selected the relevant columns to find the TopNFrequentURLsPerDay")

    relevantDF

  }


}
