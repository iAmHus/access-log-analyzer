package com.learning.analyzer.processor

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, rank}


/**
 * This class hosts the logic to generate the topN frequent URLs per day
 *
 * @param topN - the topN records you are interested in
 * @param cleanedDF - the input DataFrame to pick the topN frequent URLs per day
 */
class TopNFrequentURLsPerDayGenerator(val topN: Int, val cleanedDF: DataFrame) {


  private val logger = Logger.getLogger("com.learning.analyzer.processor.TopNFrequentURLsPerDayGenerator")

  /**
   *Used to generate the DataFrame that has the topN frequent URLs per day
   *
   * @return DataFrame that has the topN frequent URLs per day
   */
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

  /**
   * Used to select the relevant columns from the input
   *
   * @param inputDF - Input Dataframe containing all the columns
   * @return - Dataframe containing only the columns relevant to determining the topN frequent URLs per day
   */
  private def getRelevantData(inputDF: DataFrame): DataFrame = {

    val relevantDF = inputDF.select("date", "httpURL")
                            .filter("httpURL is not null")

    logger.info("Selected the relevant columns to find the TopNFrequentURLsPerDay")

    relevantDF

  }


}
