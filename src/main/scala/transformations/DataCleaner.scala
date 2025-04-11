package com.example.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{to_date, col}

object DataCleaner {
  def cleanData(df: DataFrame): DataFrame = {
    // Caste explicite des colonnes pour éviter les erreurs
    val typedDf = df.select(
      to_date(col("Date"), "yyyy-MM-dd").alias("Date"),
      col("Open").cast("double").alias("Open"),
      col("High").cast("double").alias("High"),
      col("Low").cast("double").alias("Low"),
      col("Close").cast("double").alias("Close"),
      col("Volume").cast("double").alias("Volume"),
      col("OpenInt").cast("integer").alias("OpenInt")
    )
    typedDf
      .filter(
        "Volume > 0 AND Close IS NOT NULL AND Open IS NOT NULL AND High IS NOT NULL AND Low IS NOT NULL"
      )
      .filter("High >= Low")
  }
}

