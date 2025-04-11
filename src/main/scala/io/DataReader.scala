package com.example.io

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.to_date

object DataReader {
  def readCsv(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    try {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("Date", to_date($"Date", "yyyy-MM-dd"))
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Erreur lors de la lecture du CSV : ${e.getMessage}")
    }
  }
}