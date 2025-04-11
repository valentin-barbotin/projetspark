package com.example.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.avg

object MovingAverage {
  def compute(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val movingAvg = df.withColumn(
      "MovingAvg_Close",
      avg("Close").over(
        Window.orderBy("Date").rowsBetween(-4, 0)
      )
    )
    println("=== Moyenne mobile 5 jours (Close) ===")
    movingAvg.select("Date", "Close", "MovingAvg_Close").show(20)
  }
}