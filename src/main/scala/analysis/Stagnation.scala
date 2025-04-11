package com.example.analysis

import com.example.traits.Computation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, round}

object Stagnation extends Computation {
  val stagnationThreshold = 0.5 // seuil de 0.5%

  def compute(
      df: DataFrame
  )(implicit spark: org.apache.spark.sql.SparkSession): Unit = {
    import spark.implicits._
    val stagnantDays = df
      .withColumn(
        "Stagnation",
        round((col("High") - col("Low")) / col("Low") * 100, 2)
      )
      .filter($"Stagnation" < stagnationThreshold)
    println(s"=== Jours de stagnation (variation < $stagnationThreshold%) ===")
    stagnantDays
      .select("Date", "High", "Low", "Stagnation")
      .show(truncate = false)
  }
}
