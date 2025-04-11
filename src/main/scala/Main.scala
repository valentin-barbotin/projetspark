package com.example

import com.example.config.SparkConfig
import com.example.io.DataReader
import com.example.analysis._
import com.example.traits.Computation
import com.example.transformations.Nettoyage

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.createSparkSession()
    import spark.implicits._

    val csvPath = "dataset.csv"
    val df = DataReader.readCsv(spark, csvPath)

    println("=== Données originales ===")
    df.show(5)

    // Nettoyage des données
    val cleanedDf = Nettoyage.nettoyer(df)(spark)
    println("=== Données nettoyées ===")
    cleanedDf.show()

    val computations: Seq[Computation] = Seq(
      MovingAverage,
      Volatility,
      Stagnation,
      VolumeAnalysis,
      BullishDays,
      PercentChange,
      Extremes
    )

    computations.foreach(_.compute(cleanedDf)(spark))

    // Recommandations (toutes les dates)
    Recommendations.compute(cleanedDf, "output/recommendations_all.json")(spark)

    // Recommandations (seulement la date du jour)
    Recommendations.compute(
      cleanedDf,
      "output/recommendations_today.json",
      onlyToday = true
    )(spark)

    cleanedDf.createOrReplaceTempView("stocks")
    val result = spark.sql("SELECT * FROM stocks WHERE Close > Open")
    println("=== Jours où Close > Open (via SQL) ===")
    result.show()

    spark.stop()
  }
}
