package com.learning.analyzer.processor

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, rank}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TopNFrequentVisitorsPerDayGenerator(val spark: SparkSession, val topN: Int, val cleanedDF: DataFrame) {

  def generate():DataFrame ={

    //TODO: logging
    //TODO: select only the columns interested in
    //TODO: testing

    val urlGroupedDF = cleanedDF.groupBy("date", "httpURL")
                                .agg(count(lit(1)).alias("NumOfRecordsPerURL"))


    val specForRecordsPerURL = Window.partitionBy("date")
                                     .orderBy(col("NumOfRecordsPerURL")
                                                .desc)

     urlGroupedDF.withColumn("rnk", rank().over(specForRecordsPerURL))
                                             .where(s"rnk <= $topN")
                                             .sort("date", "rnk")

  }
}
