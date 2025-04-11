package com.example.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Volatility {
  def compute(df: DataFrame): Unit = {
    val volatility = df.withColumn("Volatility", col("High") - col("Low"))
    println("=== Volatilité (High - Low) ===")
    volatility.select("Date", "High", "Low", "Volatility").show(20)
  }
}