package treat.functions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import system.{LoadTable, Properties}
import scala.annotation.tailrec

object AggregateData {
  def aggregateData(DF: DataFrame, idxStart: Int = 0): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DataAggregation")
      .getOrCreate()
    import spark.implicits._

    val DFWeather = LoadTable.load(Properties.tablePGHotelWeather)

    val colList = Seq("id_", "site_name",
      "posa_continent",
      "user_location_country",
      "user_location_region",
      "user_location_city",
      "orig_destination_distance",
      "user_id",
      "is_mobile",
      "is_package",
      "channel",
      "srch_adults_cnt",
      "srch_children_cnt",
      "srch_rm_cnt",
      "srch_destination_id",
      "srch_destination_type_id",
      "hotel_id",
      "date_time",
      "srch_ci",
      "srch_co_")

    @tailrec
    def splitDF(df: DataFrame, names: Seq[String]): DataFrame = {
      if (names.isEmpty) df
      else {
        val i = colList.length - names.length
        splitDF(
          df.withColumn(names.head, split('value, ",",-1).getItem(i)),
          names.tail
        )
      }
    }

    val DFSplitted = splitDF(DF, colList)
      .withColumn("id", regexp_replace(col("id_"), "\\[", ""))
      .withColumn("srch_co", regexp_replace(col("srch_co_"), "\\]", ""))
      .drop("id_", "srch_co_", "value")

    val joinedDF = DFSplitted.join(DFWeather, DFSplitted("hotel_id") === DFWeather("id") &&
    DFSplitted("srch_ci") === DFWeather("wthr_date"))

    val DFhot = joinedDF.filter('avg_tmpr_c >= Properties.tempLimit)
    val DFcold = joinedDF.filter('avg_tmpr_c < Properties.tempLimit)

    val DFhotAgg = DFhot.groupBy("hotel_id")
      .agg(avg("srch_adults_cnt").as("avg_hot"))

    val DFcoldAgg = DFcold.groupBy("hotel_id")
      .agg(avg("srch_adults_cnt").as("avg_cold"))

    DFhotAgg.join(DFcoldAgg, "hotel_id")
      .filter('avg_hot === 'avg_cold).show()

  }
}
