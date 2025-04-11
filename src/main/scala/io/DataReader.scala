package com.example.io

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{to_date}

object DataReader {
  def readCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .withColumn("Date", to_date($"Date", "yyyy-MM-dd"))
  }
}