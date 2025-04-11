import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TP1 Spark Scala - Analyse Financière")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val csvPath = "dataset.csv"

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)
      .withColumn("Date", F.to_date($"Date", "yyyy-MM-dd"))

    // Affichage des données originales
    println("=== Données originales ===")
    df.show(5)

    // Moyenne mobile sur 5 jours
    val movingAvg = df.withColumn(
      "MovingAvg_Close",
      F.avg("Close").over(
        Window.orderBy("Date").rowsBetween(-4, 0)
      )
    )

    println("=== Moyenne mobile 5 jours (Close) ===")
    movingAvg.select("Date", "Close", "MovingAvg_Close").show(20)

    // Volatilité (High - Low)
    val volatility = df.withColumn("Volatility", $"High" - $"Low")

    println("=== Volatilité (High - Low) ===")
    volatility.select("Date", "High", "Low", "Volatility").show(20)

    // Jour avec le plus grand volume
    val maxVolumeDay = df.orderBy($"Volume".desc).limit(1)
    println("=== Jour avec le plus grand volume ===")
    maxVolumeDay.select("Date", "Volume").show()

    // Nombre de jours où le Close > Open
    val bullishDays = df.filter($"Close" > $"Open").count()
    println(s"=== Nombre de jours haussiers (Close > Open) : $bullishDays")

    // Évolution en % du prix de clôture (d'un jour à l'autre)
    val windowSpec = Window.orderBy("Date")
    val percentChange = df.withColumn("PrevClose", F.lag("Close", 1).over(windowSpec))
      .withColumn("Pct_Change", (($"Close" - $"PrevClose") / $"PrevClose") * 100)

    println("=== Variation quotidienne (%) du prix de clôture ===")
    percentChange.select("Date", "Close", "PrevClose", "Pct_Change").show(20)

    spark.stop()
  }
}
