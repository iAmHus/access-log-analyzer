package com.learning.analyzer.main

import org.scalatest.FlatSpec

class AppTest extends FlatSpec {

  "App" should "throw an exception when the arguments passed are less than expected (in this case 4)" in {

    val exceptionWhenZeroExpctedValuesPassed = intercept[RuntimeException] {
      App.main(Array[String]())
    }

    assert(exceptionWhenZeroExpctedValuesPassed.getMessage() == "The input arguments do NOT contain the expected values - " +
      "topN ; output-directory; input URL; backup URL; please check and try again")

    val exceptionWhenOneExpctedValuesPassed = intercept[RuntimeException] {
      App.main(Array[String]("1"))
    }

    assert(exceptionWhenOneExpctedValuesPassed.getMessage() == "The input arguments do NOT contain the expected values - " +
      "topN ; output-directory; input URL; backup URL; please check and try again")

    val exceptionWhenTwoExpctedValuesPassed = intercept[RuntimeException] {
      App.main(Array[String]("1", "/tmp"))
    }

    assert(exceptionWhenTwoExpctedValuesPassed.getMessage() == "The input arguments do NOT contain the expected values - " +
      "topN ; output-directory; input URL; backup URL; please check and try again")

    val exceptionWhenThreeExpctedValuesPassed = intercept[RuntimeException] {
      App.main(Array[String]("1", "/tmp", "https://github.com/iAmHus/datasets/blob/main/NASA_access_log_Jul95.gz?raw=true"))
    }

    assert(exceptionWhenThreeExpctedValuesPassed.getMessage() == "The input arguments do NOT contain the expected values - " +
      "topN ; output-directory; input URL; backup URL; please check and try again")

  }


  "App" should "throw an exception when the input URL and the back up URL both do NOT work" in {

    val exceptionWhenThreeExpctedValuesPassed = intercept[RuntimeException] {
      App.main(Array[String]("1", "/bad_path_bad_path", "ftp://bla.bla", "ftp://bla.bla"))
    }

    assert(exceptionWhenThreeExpctedValuesPassed.getMessage() == "Can't download the file from both the input and the back-up locations")

  }

}
