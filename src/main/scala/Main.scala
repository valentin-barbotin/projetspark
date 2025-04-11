package com.example

import com.example.config.SparkConfig
import com.example.io.DataReader
import com.example.transformations.DataCleaner
import com.example.analysis._

object Main {
  def main(args: Array[String]): Unit = {
    // Initialisation de Spark
    val spark = SparkConfig.createSparkSession()
    import spark.implicits._

    // Lecture des données
    val csvPath = "dataset.csv"
    val df = DataReader.readCsv(spark, csvPath)

    println("=== Données originales ===")
    df.show(5)

    // Nettoyage des données
    val cleanedDf = DataCleaner.cleanData(df)
    println("=== Données nettoyées ===")
    cleanedDf.show()

    // Analyses
    MovingAverage.compute(spark, cleanedDf)
    Volatility.compute(cleanedDf)(spark)
    Stagnation.compute(cleanedDf)(spark)
    VolumeAnalysis.compute(cleanedDf)(spark)
    BullishDays.compute(cleanedDf)(spark)
    PercentChange.compute(cleanedDf)(spark)
    Extremes.compute(cleanedDf)(spark)

    // Recommandations (toutes les dates)
    Recommendations.compute(cleanedDf, "output/recommendations_all.json")(spark)

    // Recommandations (seulement la date du jour)
    Recommendations.compute(cleanedDf, "output/recommendations_today.json", onlyToday = true)(spark)

    // Exemple de requête SQL
    cleanedDf.createOrReplaceTempView("stocks")
    val result = spark.sql("SELECT * FROM stocks WHERE Close > Open")
    println("=== Jours où Close > Open (via SQL) ===")
    result.show()

    // Arrêt de la session Spark
    spark.stop()
  }
}