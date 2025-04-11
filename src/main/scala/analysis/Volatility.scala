package com.example.analysis

import com.example.traits.Computation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, round}

object Volatility extends Computation {
  def compute(
      df: DataFrame
  )(implicit spark: org.apache.spark.sql.SparkSession): Unit = {
    import spark.implicits._
    val volatility =
      df.withColumn("Volatility", round(col("High") - col("Low"), 2))
    println("=== Volatilité (High - Low) ===")
    volatility
      .select("Date", "High", "Low", "Volatility")
      .show(20, truncate = false)
  }
}
