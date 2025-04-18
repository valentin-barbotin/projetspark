package com.example.analysis

import com.example.traits.Computation
import org.apache.spark.sql.DataFrame

object VolumeAnalysis extends Computation {
  def compute(
      df: DataFrame
  )(implicit spark: org.apache.spark.sql.SparkSession): Unit = {
    import spark.implicits._
    val maxVolumeDay = df.orderBy($"Volume".desc).limit(1)
    println("=== Jour avec le plus grand volume ===")
    maxVolumeDay.select("Date", "Volume").show(truncate = false)
  }
}
