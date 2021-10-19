package com.learning.analyzer.processor

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, rank}

/**
 * This class hosts the logic to generate the topN frequent visitors per day
 *
 * @param topN - the topN records you are interested in
 * @param cleanedDF - the input DataFrame to pick the topN frequent visitors per day
 */
class TopNFrequentVisitorsPerDayGenerator(val topN: Int, val cleanedDF: DataFrame) {

  private val logger = Logger.getLogger("com.learning.analyzer.processor.TopNFrequentVisitorsPerDayGenerator")


  /**
   * Used to generate the DataFrame that has the topN frequent visitors per day
   *
   * @return DataFrame that has the topN frequent visitors per day
   */
  def generate(): DataFrame = {

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

  /**
   * Used to select the relevant columns from the input
   *
   * @param inputDF - Input Dataframe containing all the columns
   * @return - Dataframe containing only the columns relevant to determining the topN frequent visitors per day
   */

  private def getRelevantData(inputDF: DataFrame): DataFrame = {

    val relevantDF = inputDF.select("date", "remoteHost")
                            .filter("remoteHost is not null")

    logger.info("Selected the relevant columns to find the TopNFrequentVisitorsPerDay")

    relevantDF

  }

}
