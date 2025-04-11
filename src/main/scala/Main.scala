import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TP1 Spark Scala")
      .master("local[*]")
      .getOrCreate()

    val data = Seq("Scala", "Spark", "TP1")
    val df = spark.createDataFrame(data.map(Tuple1(_))).toDF("value")

    df.show()

    spark.stop()
  }
}