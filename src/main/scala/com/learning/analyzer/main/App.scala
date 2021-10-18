package com.learning.analyzer.main

import com.learning.analyzer.preprocessor.Preprocessor
import com.learning.analyzer.processor.{CountGenerator, TopNFrequentURLsPerDayGenerator, TopNFrequentVisitorsPerDayGenerator}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths}
import scala.sys.process._

object App {

  val logger = Logger.getLogger("com.learning.analyzer.main.App")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                            .appName("App Log Scanner")
                            .master("local")
                            .config("spark.conf.logLevel", "ERROR")
                            .getOrCreate()



    spark.conf.set("spark.sql.shuffle.partitions", "255")
    spark.sqlContext.setConf("spark.sql.files.ignoreCorruptFiles", "true")
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Started and created a spark session")

    //TODO: logging

    validateInputArgs(args, logger)

    val topN = args(0).toInt
    val inputFile = s"${args(1)}/test.gz"
    val outputFileDir = s"${args(1)}/output"
    val fileUrl = args(2)

    saveInputFile(inputFile, fileUrl)


    val cleanedDF = new Preprocessor(spark, inputFile).preprocess()

    //cleanedDF.cache()

/*
    new CountGenerator(spark, topN,
                       cleanedDF, outputFileDir).processData()*/


    val topNFrequentURLsPerDay = new TopNFrequentURLsPerDayGenerator(spark, topN, cleanedDF).generate()
    val topNFrequentVisitorsPerDay = new TopNFrequentVisitorsPerDayGenerator(spark, topN, cleanedDF).generate()

    logger.info(s"##### - $outputFileDir")
    /*topNFrequentURLsPerDay.coalesce(1)
                          .write
                          .option("header", "true")
                          .mode(SaveMode
                                  .Overwrite)
                          .csv(s"$outputFileDir/topNFrequentURLsPerDay")*/

    writeToDisk(s"${outputFileDir}/topNFrequentURLsPerDay",
                topNFrequentURLsPerDay)

    writeToDisk(s"${outputFileDir}/topNFrequentVisitorsPerDay",
                topNFrequentVisitorsPerDay)

   // cleanedDF.unpersist()

    //TODO: add it in finally block

    spark.stop()

    logger.info("Process completed successfully")
  }

  private def saveInputFile(inputFile: String, fileUrl: String): Unit = {
    try {
      if (Files.notExists(Paths.get(inputFile))) {
        new URL(fileUrl) #> new File(inputFile) !!
      }
    } catch {
      case _ : Exception => saveInputFileFromBackupLocation(inputFile, BackupInputURL)
    }
  }

  private def saveInputFileFromBackupLocation(inputFile: String, fileUrl: String): Unit ={

    try {
      if (Files.notExists(Paths.get(inputFile))) {
        new URL(fileUrl) #> new File(inputFile) !!
      }
    } catch {
      case _ : Exception => {

      }
    }
  }

  private def writeToDisk(outputFile: String,
                          outputDataFrame: DataFrame) = {
    outputDataFrame.coalesce(1)
                   .write
                   .option("header", "true")
                   .mode(SaveMode.Overwrite)
                   .csv(outputFile)
  }

  private def validateInputArgs(args: Array[String],
                                logger: Logger) = {
    if (args
      .length != 3 || args.filter(_.nonEmpty).length != args.length) {
      logger.error("The input arguments do NOT contain the expected values - topN ; bind-volume-directory; URL; please check and try again")
      System.exit(1)
    }
  }
}
