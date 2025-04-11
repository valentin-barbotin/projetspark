package com.example.transformations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Nettoyage {

  def nettoyer(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // 1. Cast explicite des colonnes + conversion de la date
    val typedDf = df.select(
      to_date(col("Date"), "yyyy-MM-dd").alias("Date"),
      col("Open").cast("double").alias("Open"),
      col("High").cast("double").alias("High"),
      col("Low").cast("double").alias("Low"),
      col("Close").cast("double").alias("Close"),
      col("Volume").cast("double").alias("Volume"),
      col("OpenInt").cast("integer").alias("OpenInt")
    )

    // 2. Supprimer les doublons
    val dfDedup = typedDf.dropDuplicates()

    // 3. Filtrer les lignes avec des valeurs nulles ou incoherentes
    val dfFiltre = dfDedup
      .filter(
        $"Open".isNotNull && $"High".isNotNull && $"Low".isNotNull &&
          $"Close".isNotNull && $"Volume".isNotNull &&
          $"Open" >= 0 && $"High" >= 0 && $"Low" >= 0 &&
          $"Close" >= 0 && $"Volume" > 0 && $"High" >= $"Low"
      )

    dfFiltre
  }
}
