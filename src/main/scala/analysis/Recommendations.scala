package com.example.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, when, current_date, lit}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.avg

object Recommendations {
  def compute(df: DataFrame, outputPath: String, onlyToday: Boolean = false)(
      implicit spark: SparkSession
  ): Unit = {
    import spark.implicits._

    // Calcul de la moyenne mobile sur 5 jours
    val movingAvg = df.withColumn(
      "MovingAvg_Close",
      avg("Close").over(
        Window.orderBy("Date").rowsBetween(-4, 0)
      )
    )

    // Logique de recommandation
    val recommendationDf = movingAvg.withColumn(
      "Recommendation",
      when(
        $"Close" > $"MovingAvg_Close" * 1.01,
        "buy"
      ) // Acheter si Close > 101% de la moyenne mobile
        .when(
          $"Close" < $"MovingAvg_Close" * 0.99,
          "sell"
        ) // Vendre si Close < 99% de la moyenne mobile
        .otherwise("hold") // Garder sinon
    )

    // Filtrer pour la date du jour si onlyToday est true
    val finalDf = if (onlyToday) {
      recommendationDf.filter($"Date" === current_date())
    } else {
      recommendationDf
    }

    // Sélectionner les colonnes pertinentes
    val resultDf =
      finalDf.select("Date", "Close", "MovingAvg_Close", "Recommendation")

    // Afficher les résultats
    println(s"=== Recommandations (${if (onlyToday) "date du jour"
      else "toutes les dates"}) ===")
    resultDf.show()

    // Écrire les résultats en JSON
    resultDf.write
      .mode("overwrite")
      .json(outputPath)

    println(s"Résultats écrits dans $outputPath")
  }
}

