package com.learning.analyzer.processor

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}


class TopNFrequentURLsPerDayGeneratorTest extends FlatSpec with BeforeAndAfter{


  "TopNFrequentURLsPerDayGeneratorTest" should "return CORRECT values for count" in {

    val spark = SparkSession.builder
                            .master("local")
                            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val topN = 4

    val inputDF = Seq(
      ("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com")
      ).toDF("date", "httpURL")

    val topNFrequentURLsPerDay = new TopNFrequentURLsPerDayGenerator(topN, inputDF).generate()


    assertResult(3, "NumOfRecordsPerURL for date = \"1995-07-03\" AND remoteHost=\"alyssa.prodigy.com\"")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-03\" AND httpURL=\"alyssa.prodigy.com\"")
                                               .select("NumOfRecordsPerURL")
                                               .first().getLong(0))

    assertResult(2, "NumOfRecordsPerURL for date = \"1995-07-05\" AND remoteHost=\"piweba3y.prodigy.com\"")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-05\" AND httpURL=\"piweba3y.prodigy.com\"")
                                               .select("NumOfRecordsPerURL")
                                               .first().getLong(0))

    spark.stop()

  }

  "TopNFrequentURLsPerDayGeneratorTest" should "return CORRECT values for rnk" in {

    val spark = SparkSession.builder
                            .master("local")
                            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val topN = 2

    val inputDF = Seq(
      ("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-05", "alyssa.prodigy.com"),
      ("1995-07-05", "alyssa.prodigy.com"),
      ("1995-07-03", "piweba3y.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-05", "alyssa.prodigy.com")
      ).toDF("date", "httpURL")

    val topNFrequentURLsPerDay = new TopNFrequentURLsPerDayGenerator(topN, inputDF).generate()

    assertResult(2, "Rank for date = \"1995-07-05\" AND remoteHost=\"piweba3y.prodigy.com\"")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-05\" AND httpURL=\"piweba3y.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))


    assertResult(1, "Rank for date = \"1995-07-05\" AND remoteHost=\"alyssa.prodigy.com\"")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-05\" AND httpURL=\"alyssa.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))

    assertResult(1, "Rank for date = \"1995-07-03\" AND remoteHost=\"alyssa.prodigy.com\"")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-03\" AND httpURL=\"alyssa.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))

    assertResult(2, "Rank for date = \"1995-07-03\" AND remoteHost=\"piweba3y.prodigy.com\"")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-03\" AND httpURL=\"piweba3y.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))

    spark.stop()
  }

}
