package com.learning.analyzer.processor

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}


class TopNFrequentVisitorsPerDayGeneratorTest extends FlatSpec with BeforeAndAfter{

  "TopNFrequentVisitorsPerDayGeneratorTest" should "return CORRECT values for count" in {

    val spark = SparkSession.builder
                            .master("local")
                            .config("spark.sql.shuffle.partitions", "1")

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
      ).toDF("date", "remoteHost")

    val topNFrequentURLsPerDay = new TopNFrequentVisitorsPerDayGenerator(topN, inputDF).generate()


    assertResult(3, "NumOfRecordsPerHost for date = \"1995-07-03\" AND remoteHost=\"alyssa.prodigy.com\"")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-03\" AND remoteHost=\"alyssa.prodigy.com\"")
                                               .select("NumOfRecordsPerHost")
                                               .first().getLong(0))

    assertResult(2, "NumOfRecordsPerHost for date = \"1995-07-05\" AND remoteHost=\"piweba3y.prodigy.com\"")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-05\" AND remoteHost=\"piweba3y.prodigy.com\"")
                                               .select("NumOfRecordsPerHost")
                                               .first().getLong(0))
    spark.stop()

  }

  "TopNFrequentVisitorsPerDayGeneratorTest" should "return CORRECT values for rnk" in {

    val spark = SparkSession.builder
                            .master("local")
                            .config("spark.sql.shuffle.partitions", "1")

                            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val topN = 4

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

    val topNFrequentVisitorsPerDay = new TopNFrequentVisitorsPerDayGenerator(topN, inputDF).generate()


    assertResult(2, "Rank for date = \"1995-07-05\" AND remoteHost=\"piweba3y.prodigy.com\"")(topNFrequentVisitorsPerDay
                                               .filter("date = \"1995-07-05\" AND remoteHost=\"piweba3y.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))


    assertResult(1, "Rank for date = \"1995-07-05\" AND remoteHost=\"alyssa.prodigy.com\"")(topNFrequentVisitorsPerDay
                                               .filter("date = \"1995-07-05\" AND remoteHost=\"alyssa.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))

    assertResult(1, "Rank for date = \"1995-07-03\" AND remoteHost=\"alyssa.prodigy.com\"")(topNFrequentVisitorsPerDay
                                               .filter("date = \"1995-07-03\" AND remoteHost=\"alyssa.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))

    assertResult(2, "Rank for date = \"1995-07-03\" AND remoteHost=\"piweba3y.prodigy.com\"")(topNFrequentVisitorsPerDay
                                               .filter("date = \"1995-07-03\" AND remoteHost=\"piweba3y.prodigy.com\"")
                                               .select("rnk")
                                               .first().getInt(0))
    spark.stop()

  }


  "TopNFrequentVisitorsPerDayGeneratorTest" should "ignore rows that have NULL column for httpURL" in {

    val spark = SparkSession.builder
                            .master("local")
                            .config("spark.sql.shuffle.partitions", "1")

                            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val topN = 2

    val inputDF = Seq(
      ("1995-07-05", null),
      ("1995-07-05", null),

      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com")
      ).toDF("date", "remoteHost")

    val topNFrequentVisitorsPerDay = new TopNFrequentVisitorsPerDayGenerator(topN, inputDF).generate()

    assertResult(0, "Should ignore rows where remoteHost is null ")(topNFrequentVisitorsPerDay
                                                                                                .filter("date = \"1995-07-05\"")
                                                                                                .count())

  }
}
