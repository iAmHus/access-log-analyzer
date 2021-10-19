package com.learning.analyzer.main

import com.learning.analyzer.global.Constants
import com.learning.analyzer.preprocessor.Preprocessor
import com.learning.analyzer.processor.{TopNFrequentURLsPerDayGenerator, TopNFrequentVisitorsPerDayGenerator}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths}
import scala.sys.process._

object App {

  private val logger = Logger.getLogger("com.learning.analyzer.main.App")

  def main(args: Array[String]): Unit = {
    var spark = None: Option[SparkSession]

    try {

      validateInputArgs(args)

      spark = Some(SparkSession.builder()
                               .appName("App Log Scanner")
                               .master("local")
                               .config("spark.conf.logLevel", "ERROR")
                               .getOrCreate())

      spark.getOrElse(throw new RuntimeException("Can't proceed without a SparkSession"))

      spark.get.conf.set("spark.sql.shuffle.partitions", "255")
      spark.get.sqlContext.setConf("spark.sql.files.ignoreCorruptFiles", "true")
      spark.get.sparkContext.setLogLevel("ERROR")

      logger.info("Started and created a spark session")


      val topN = args(0).toInt
      val inputFile = s"${args(1)}/test.gz"
      val outputFileDir = s"${args(1)}/output"
      val fileUrl = args(2)

      saveInputFile(inputFile, fileUrl)


      val cleanedDF = new Preprocessor(spark.get, inputFile).preprocess()

      cleanedDF.cache()

      val topNFrequentURLsPerDay = new TopNFrequentURLsPerDayGenerator(topN, cleanedDF).generate()
      val topNFrequentVisitorsPerDay = new TopNFrequentVisitorsPerDayGenerator(topN, cleanedDF).generate()

      logger.info("Saving the output files to disk started")

      writeToDisk(s"$outputFileDir/topNFrequentURLsPerDay",
                  topNFrequentURLsPerDay)

      writeToDisk(s"$outputFileDir/topNFrequentVisitorsPerDay",
                  topNFrequentVisitorsPerDay)

      logger.info("Saving the output files to disk completed")

      cleanedDF.unpersist()

      logger.info("Process completed successfully")

    } catch {
      case e: Exception => {
        logger.error(s"${e.getMessage} occurred while processing the logs and the trace is - ${e.printStackTrace()}")
      }
    } finally {
      if (spark.isDefined) spark.get.stop()
    }

  }

  /**
   * Used to get the input file from the input location passed
   * In case it is not retrieved, the program goes to a backup location
   *
   * @param inputFile - Location to write input file to
   * @param fileUrl - Input file URL passed to the program
   */
  private def saveInputFile(inputFile: String, fileUrl: String): Unit = {
    try {
      if (Files.notExists(Paths.get(inputFile))) {
        new URL(fileUrl) #> new File(inputFile) !!
      }
      logger.info("Saving the file from the input location completed")
    } catch {
      case _: Exception => {
        logger.error(s"Can't download the file from the location in the startup args : $fileUrl, " +
                       s"trying the backup location - ${Constants.BackupInputURL}")
        Files.deleteIfExists(Paths.get(inputFile))
        getInputFileFromBackupLocation(inputFile,
                                       Constants.BackupInputURL)
      }
    }
  }

  /**
   * Used to get the input file from the backup location.
   * In case it is not retrieved, the program fails with an error
   *
   * @param inputFile - Location to write input file to
   * @param fileUrl - Back up file location
   */
  private def getInputFileFromBackupLocation(inputFile: String, fileUrl: String): Unit = {

    try {
      if (Files.notExists(Paths.get(inputFile))) {
        new URL(fileUrl) #> new File(inputFile) !!
      }
      logger.info("Saving the file from the backup location completed")

    } catch {
      case _: Exception => {
        logger.error(s"Can't download the file from the back-up location : $fileUrl, shutting down now !")
        System.exit(1)
      }
    }
  }

  /**
   * Used to write a DataFrame to the output file specified
   *
   * @param outputFile - Output file to write to
   * @param outputDataFrame - Dataframe to write
   */
  private def writeToDisk(outputFile: String,
                          outputDataFrame: DataFrame) = {
    outputDataFrame.coalesce(1)
                   .write
                   .option("header", "true")
                   .mode(SaveMode
                           .Overwrite)
                   .csv(outputFile)
  }

  /**
   * Used to validate that the expected number of arguments are passed to the program and none of them are valid
   *
   * @param args - Input arguments passed to the spark program
   */
  private def validateInputArgs(args: Array[String]) = {
    if (args.length != 3 || args.count(_.nonEmpty) != args.length) {
      logger.error("The input arguments do NOT contain the expected values - topN ; output-directory; URL; please check and try again")
      System.exit(1)
    }
  }
}
