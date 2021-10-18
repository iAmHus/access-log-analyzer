package com.learning.analyzer.processor

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, rank}

class TopNFrequentVisitorsPerDayGenerator(val topN: Int, val cleanedDF: DataFrame) {

  private val logger = Logger.getLogger("com.learning.analyzer.processor.TopNFrequentVisitorsPerDayGenerator")

  def generate(): DataFrame = {

    //TODO: logging
    logger.info("Process to determine the TopNFrequentVisitorsPerDay started")

    val relevantDF = getRelevantData(cleanedDF)

    val hostGroupedDF = relevantDF.groupBy("date", "remoteHost")
                                  .agg(count(lit(1)).alias("NumOfRecordsPerHost"))


    val specForRecordsPerHost = Window.partitionBy("date")
                                      .orderBy(col("NumOfRecordsPerHost")
                                                 .desc)

    val topNFrequentVisitorsPerDay = hostGroupedDF.withColumn("rnk", rank().over(specForRecordsPerHost))
                                                  .where(s"rnk <= $topN")
                                                  .sort("date", "rnk")

    logger.info("Process to determine the TopNFrequentVisitorsPerDay completed")

    topNFrequentVisitorsPerDay
  }

  private def getRelevantData(inputDF: DataFrame): DataFrame = {

    val relevantDF = inputDF.select("date", "remoteHost")
                            .filter("remoteHost is not null")

    logger.info("Selected the relevant columns to find the TopNFrequentVisitorsPerDay")

    relevantDF

  }

}
