import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TP1 Spark Scala - Lecture CSV")
      .master("local[*]")
      .getOrCreate()

    val csvPath = "seattle-weather.csv"

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    df.show()

    spark.stop()
  }
}
