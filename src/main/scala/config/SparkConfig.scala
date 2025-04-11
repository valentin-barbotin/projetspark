package com.example.config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  def createSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("TP1 Spark Scala - Analyse Financière")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    // Réduire les logs ennuyant
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}

