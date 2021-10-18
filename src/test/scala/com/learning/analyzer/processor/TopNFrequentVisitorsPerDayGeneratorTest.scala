package com.learning.analyzer.processor

import com.learning.analyzer.utils.DataFrameUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec


class TopNFrequentURLsPerDayGeneratorTest extends AnyFlatSpec with BeforeAndAfter{

  var spark:SparkSession = _

  before {
    spark = SparkSession.builder
                        .appName("TopNFrequentURLsPerDayGeneratorTest")
                        .master("local[*]")
                        .getOrCreate()

    import spark.implicits._

  }

  "TopNFrequentURLsPerDayGenerator" should "return valid counts" in {

    spark.sparkContext.setLogLevel("ERROR")

    val topN = 4


    val inputDF = Seq(("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-05", "piweba3y.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com"),
      ("1995-07-03", "alyssa.prodigy.com")
      ).toDF("date", "remoteHost")

    val topNFrequentURLsPerDay = new TopNFrequentURLsPerDayGenerator(spark, topN, inputDF).generate()


    assertResult(3, "num of missing values")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-03\"")
                                               .select("NumOfRecordsPerHost")
                                               .first().getLong(0))

    assertResult(2, "num of missing values")(topNFrequentURLsPerDay
                                               .filter("date = \"1995-07-05\"")
                                               .select("NumOfRecordsPerHost")
                                               .first().getLong(0))

  }

}
