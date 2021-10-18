package com.learning.analyzer.processor

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec


class TopNFrequentVisitorsPerDayGeneratorTest extends AnyFlatSpec with BeforeAndAfter{


    val spark = SparkSession.builder
                        .appName("TopNFrequentURLsPerDayGeneratorTest")
                        .master("local[*]")
                        .getOrCreate()


  "TopNFrequentVisitorsPerDayGeneratorTest" should "return CORRECT values for count" in {

    spark.sparkContext.setLogLevel("ERROR")

    val topN = 4

    import spark.implicits._

    val inputDF = Seq(
      ("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com")
      ).toDF("date", "remoteHost")

    val topNFrequentURLsPerDay = new TopNFrequentVisitorsPerDayGenerator(spark, topN, inputDF).generate()


    assertResult(3, "num of missing values")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-03\" AND remoteHost=\"alyssa.prodigy.com\"")
                                               .select("NumOfRecordsPerHost")
                                               .first().getLong(0))

    assertResult(2, "num of missing values")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-05\" AND remoteHost=\"piweba3y.prodigy.com\"")
                                               .select("NumOfRecordsPerHost")
                                               .first().getLong(0))

  }

  "TopNFrequentVisitorsPerDayGeneratorTest" should "return CORRECT values for rnk" in {

    spark.sparkContext.setLogLevel("ERROR")

    val topN = 4

    import spark.implicits._

    val inputDF = Seq(
      ("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-05", "alyssa.prodigy.com"),
      ("1995-07-05", "alyssa.prodigy.com"),
      ("1995-07-03", "piweba3y.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-05", "alyssa.prodigy.com")
      ).toDF("date", "remoteHost")

    val topNFrequentURLsPerDay = new TopNFrequentVisitorsPerDayGenerator(spark, topN, inputDF).generate()


    topNFrequentURLsPerDay.show()

    assertResult(1, "num of missing values")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-05\" AND remoteHost=\"piweba3y.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))


    assertResult(2, "num of missing values")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-05\" AND remoteHost=\"alyssa.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))

    assertResult(1, "num of missing values")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-03\" AND remoteHost=\"alyssa.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))

    assertResult(2, "num of missing values")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-03\" AND remoteHost=\"piweba3y.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))

  }


  after {
    spark.stop()
  }
}
