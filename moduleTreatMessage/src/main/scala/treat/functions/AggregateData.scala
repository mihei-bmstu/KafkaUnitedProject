package treat.functions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import system.{LoadTable, Properties}

object AggregateData {
  def aggregateData(DF: DataFrame, idxStart: Int = 0): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DataAggregation")
      .getOrCreate()
    import spark.implicits._
    val DFWeather = LoadTable.load(Properties.tablePGHotelWeather)
    val DFSplitted = DF.select(split('value, ",").getItem(0).as("id_"),
      split('value, ",").getItem(1).as("site_name"),
      split('value, ",").getItem(2).as("posa_continent"),
      split('value, ",").getItem(3).as("user_location_country"),
      split('value, ",").getItem(4).as("user_location_region"),
      split('value, ",").getItem(5).as("user_location_city"),
      split('value, ",").getItem(6).as("orig_destination_distance"),
      split('value, ",").getItem(7).as("user_id"),
      split('value, ",").getItem(8).as("is_mobile"),
      split('value, ",").getItem(9).as("is_package"),
      split('value, ",").getItem(10).as("channel"),
      split('value, ",").getItem(11).as("srch_adults_cnt"),
      split('value, ",").getItem(12).as("srch_children_cnt"),
      split('value, ",").getItem(13).as("srch_rm_cnt"),
      split('value, ",").getItem(14).as("srch_destination_id"),
      split('value, ",").getItem(15).as("srch_destination_type_id"),
      split('value, ",").getItem(16).as("hotel_id"),
      split('value, ",").getItem(17).as("date_time"),
      split('value, ",").getItem(18).as("srch_ci"),
      split('value, ",").getItem(19).as("srch_co_"))
      .withColumn("id", regexp_replace(col("id_"), "\\[", ""))
      .withColumn("srch_co", regexp_replace(col("srch_co_"), "\\]", ""))
      .drop("id_", "srch_co_")

    val joinedDF = DFSplitted.join(DFWeather, DFSplitted("hotel_id") === DFWeather("id") &&
    DFSplitted("srch_ci") === DFWeather("wthr_date"))

    val DFhot = joinedDF.filter('avg_tmpr_c > 20)
    val DFcold = joinedDF.filter('avg_tmpr_c < 20)

    DFhot.show()
    DFcold.show()
  }
}
