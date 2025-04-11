package com.example.transformations

import org.apache.spark.sql.DataFrame

object DataCleaner {
  def cleanData(df: DataFrame): DataFrame = {
    df.filter("Volume > 0 AND Close IS NOT NULL AND Open IS NOT NULL AND High IS NOT NULL AND Low IS NOT NULL")
      .filter("High >= Low")
  }
}