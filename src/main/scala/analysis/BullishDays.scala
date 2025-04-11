package com.example.analysis

import com.example.traits.Computation
import org.apache.spark.sql.DataFrame

object BullishDays extends Computation {
  def compute(
      df: DataFrame
  )(implicit spark: org.apache.spark.sql.SparkSession): Unit = {
    import spark.implicits._
    val bullishDays = df.filter($"Close" > $"Open").count()
    println(s"=== Nombre de jours haussiers (Close > Open) : $bullishDays")
  }
}

