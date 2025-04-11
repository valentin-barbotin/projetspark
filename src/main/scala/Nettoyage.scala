import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Nettoyage {

  def nettoyer(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // 1. Supprimer les doublons
    val dfDedup = df.dropDuplicates()

    // 2. Filtrer les lignes avec des valeurs nulles ou negatives
    val dfFiltre = dfDedup.filter(
      $"Open".isNotNull && $"High".isNotNull && $"Low".isNotNull &&
        $"Close".isNotNull && $"Volume".isNotNull &&
        $"Open" >= 0 && $"High" >= 0 && $"Low" >= 0 &&
        $"Close" >= 0 && $"Volume" >= 0
    )

    // 3. Convertir la colonne Date en format DateType
    val dfAvecDate = dfFiltre.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

    // 4. Supprimer les colonnes constantes (ex: OpenInt si elle est toujours 0)
    val distinctOpenInt = dfAvecDate.select("OpenInt").distinct().count()
    val dfFinal = if (distinctOpenInt <= 1) dfAvecDate.drop("OpenInt") else dfAvecDate

    dfFinal
  }
}
