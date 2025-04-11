package com.example.traits

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Computation {
  def compute(df: DataFrame)(implicit spark: SparkSession): Unit
}
