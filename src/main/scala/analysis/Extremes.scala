package com.example.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, desc}

object Extremes {
  def compute(df: DataFrame)(implicit spark: org.apache.spark.sql.SparkSession): Unit = {
    import spark.implicits._ 
    val maxClose = df.orderBy(desc("Close")).select("Date", "Close").first()
    val minClose = df.orderBy(asc("Close")).select("Date", "Close").first()

    println(s"=== Valeur maximale ===\nDate: ${maxClose.getAs[String]("Date")}, Close: ${maxClose.getAs[Double]("Close")}")
    println(s"=== Valeur minimale ===\nDate: ${minClose.getAs[String]("Date")}, Close: ${minClose.getAs[Double]("Close")}")
  }
}