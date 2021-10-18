package com.learning.analyzer.processor

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, rank}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TopNFrequentURLsPerDayGenerator (val spark: SparkSession, val topN: Int, val cleanedDF: DataFrame) {

  def generate():DataFrame ={

    //TODO: logging
    //TODO: select only the columns interested in
    //TODO: testing

    val hostGroupedDF = cleanedDF.groupBy("date", "remoteHost")
                                 .agg(count(lit(1)).alias("NumOfRecordsPerHost"))


    val specForRecordsPerHost = Window.partitionBy("date")
                                      .orderBy(col("NumOfRecordsPerHost")
                                                 .desc)

    hostGroupedDF.withColumn("rnk", rank().over(specForRecordsPerHost))
                                                  .where(s"rnk <= $topN")
                                                  .sort("date", "rnk")

  }
}
