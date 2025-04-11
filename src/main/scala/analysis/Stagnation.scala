package com.example.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Stagnation {
  val stagnationThreshold = 0.5 // seuil de 0.5%

  def compute(df: DataFrame)(implicit spark: org.apache.spark.sql.SparkSession): Unit = {
    import spark.implicits._ 
    val stagnantDays = df.withColumn("Stagnation", (col("High") - col("Low")) / col("Low") * 100)
      .filter($"Stagnation" < stagnationThreshold)
    println("=== Jours de stagnation ===")
    stagnantDays.select("Date", "High", "Low", "Stagnation").show()
  }
}