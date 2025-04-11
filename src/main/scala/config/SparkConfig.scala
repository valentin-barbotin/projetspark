package com.example.config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("TP1 Spark Scala - Analyse Financière")
      .master("local[*]")
      .getOrCreate()
  }
}