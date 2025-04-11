package com.example.config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("TP1 Spark Scala - Analyse Financière")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") // Optimisation pour local[*]
      .getOrCreate()

    // Réduire les logs verbeux
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}