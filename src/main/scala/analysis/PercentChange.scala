package com.example.analysis

import com.example.traits.Computation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, round}

object PercentChange extends Computation {
  def compute(
      df: DataFrame
  )(implicit spark: org.apache.spark.sql.SparkSession): Unit = {
    import spark.implicits._
    val windowSpec = Window.orderBy("Date")
    val percentChange = df
      .withColumn("PrevClose", lag("Close", 1).over(windowSpec))
      .withColumn(
        "Pct_Change",
        round((($"Close" - $"PrevClose") / $"PrevClose") * 100, 2)
      )
    println("=== Variation quotidienne (%) du prix de clôture ===")
    percentChange
      .select("Date", "Close", "PrevClose", "Pct_Change")
      .show(20, truncate = false)
  }
}
