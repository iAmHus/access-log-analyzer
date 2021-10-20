package com.learning.analyzer.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, when}


object DataFrameUtils {

  def isEqual(df1: DataFrame, df2: DataFrame): Boolean = {
   isSchemaEqual(df1, df2) && areElemsEqual(df1, df2)

  }
  def isSchemaEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    df1.schema.equals(df2.schema)

  }

  def areElemsEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    df1.collect().sameElements(df2.collect())

  }

  def getEmptyValCounts(inputDF: DataFrame): DataFrame = {

    //Reference : https://sparkbyexamples.com/spark/spark-find-count-of-null-empty-string-values/
    val nullOrEmptyColCounts = inputDF.columns.map(c => {
      count(when(col(c).isNull ||
               col(c) === "" ||
               col(c).contains("NULL") ||
               col(c).contains("null"), c)
        ).alias(c)
    })

    println(nullOrEmptyColCounts)
    println("**"*5)
    inputDF.select(nullOrEmptyColCounts: _*).show()

    inputDF.select(nullOrEmptyColCounts: _*)
  }


}