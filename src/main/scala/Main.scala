import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TP1 Spark Scala - Analyse Financière")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    Logger.getLogger("org").setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("ERROR")

    val csvPath = "dataset.csv"

    // === Lecture brute du CSV ===
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    println("=== Aperçu brut des données ===")
    df.show(5)

    // Comparatif avant nettoyage
    println(s"Nombre de lignes AVANT nettoyage : ${df.count()}")
    println(s"Nombre de doublons AVANT nettoyage : ${df.count() - df.dropDuplicates().count()}")

    println("=== Valeurs nulles par colonne (brutes) ===")
    df.columns.foreach { colName =>
      val nullCount = df.filter(F.col(colName).isNull).count()
      println(f"$colName: $nullCount nulls")
    }

    println("=== Statistiques AVANT nettoyage ===")
    df.describe("Open", "Close", "Volume").show()

    // === Nettoyage via Nettoyage.scala ===
    val cleanedDf = Nettoyage.nettoyer(df)(spark)

    println("\n=== Aperçu après nettoyage ===")
    cleanedDf.show(5)

    println(s"Nombre de lignes APRÈS nettoyage : ${cleanedDf.count()}")
    println(s"Nombre de doublons APRÈS nettoyage : ${cleanedDf.count() - cleanedDf.dropDuplicates().count()}")

    println("=== Valeurs nulles par colonne (nettoyées) ===")
    cleanedDf.columns.foreach { colName =>
      val nullCount = cleanedDf.filter(F.col(colName).isNull).count()
      println(f"$colName: $nullCount nulls")
    }

    println("=== Statistiques APRÈS nettoyage ===")
    cleanedDf.describe("Open", "Close", "Volume").show()

    // === Moyenne mobile sur 5 jours ===
    val movingAvg = cleanedDf.withColumn(
      "MovingAvg_Close",
      F.avg("Close").over(Window.orderBy("Date").rowsBetween(-4, 0))
    )

    println("=== Moyenne mobile 5 jours (Close) ===")
    movingAvg.select("Date", "Close", "MovingAvg_Close").show(20)

    // === Volatilité (High - Low) ===
    val volatility = cleanedDf.withColumn("Volatility", $"High" - $"Low")

    println("=== Volatilité (High - Low) ===")
    volatility.select("Date", "High", "Low", "Volatility").show(20)

    // === Détection de stagnation ===
    val stagnationThreshold = 0.5
    val stagnantDays = cleanedDf.withColumn("Stagnation", ($"High" - $"Low") / $"Low" * 100)
      .filter($"Stagnation" < stagnationThreshold)

    println("=== Jours de stagnation (< 0.5%) ===")
    stagnantDays.select("Date", "High", "Low", "Stagnation").show()

    // === Jour avec le plus grand volume ===
    val maxVolumeDay = cleanedDf.orderBy($"Volume".desc).limit(1)
    println("=== Jour avec le plus grand volume ===")
    maxVolumeDay.select("Date", "Volume").show()

    // === Nombre de jours haussiers (Close > Open) ===
    val bullishDays = cleanedDf.filter($"Close" > $"Open").count()
    println(s"=== Nombre de jours haussiers (Close > Open) : $bullishDays")

    // === Évolution quotidienne en % du Close ===
    val windowSpec = Window.orderBy("Date")
    val percentChange = cleanedDf.withColumn("PrevClose", F.lag("Close", 1).over(windowSpec))
      .withColumn("Pct_Change", (($"Close" - $"PrevClose") / $"PrevClose") * 100)

    println("=== Variation quotidienne (%) du prix de clôture ===")
    percentChange.select("Date", "Close", "PrevClose", "Pct_Change").show(20)

    // === Valeur maximale / minimale de Close ===
    val maxClose = cleanedDf.orderBy(F.desc("Close")).select("Date", "Close").first()
    val minClose = cleanedDf.orderBy(F.asc("Close")).select("Date", "Close").first()

    println(s"=== Valeur maximale ===\nDate: ${maxClose.getAs[String]("Date")}, Close: ${maxClose.getAs[Double]("Close")}")
    println(s"=== Valeur minimale ===\nDate: ${minClose.getAs[String]("Date")}, Close: ${minClose.getAs[Double]("Close")}")

    // === SQL Temp View ===
    cleanedDf.createOrReplaceTempView("stocks")
    val result = spark.sql("SELECT * FROM stocks WHERE Close > Open")
    println("=== Requête SQL : jours haussiers ===")
    result.show()

    spark.stop()
  }
}
