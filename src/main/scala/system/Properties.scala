package system

import org.apache.spark.sql.types._
import java.util.Properties

object Properties {

  val propertiesPG = new Properties()
  propertiesPG.setProperty("user", "user")
  propertiesPG.setProperty("password", "user")
  propertiesPG.setProperty("driver", "org.postgresql.Driver")

  val urlPG = "jdbc:postgresql://localhost:5432/demo"
  val pathExpedia = "C:\\Users\\mvchernov\\work\\201source\\07\\expedia\\"
  val pathHotelWeather = "C:\\Users\\mvchernov\\work\\201source\\07\\hotel-weather\\"
  val tablePGExpedia = "expedia"
  val tablePGHotelWeather = "hotel_weather"

  val propertiesKafka = new Properties()
  propertiesKafka.put("bootstrap.servers", "localhost:9092")
  propertiesKafka.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  propertiesKafka.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val kafkaTopic = "testtopic"

  val schemaExpedia: StructType = StructType(
    StructField("id", LongType, nullable = false) ::
      StructField("date_time", DateType, nullable = true) ::
      StructField("site_name", IntegerType, nullable = true) ::
      StructField("posa_continent", IntegerType, nullable = true) ::
      StructField("user_location_country", IntegerType, nullable = true) ::
      StructField("user_location_region", IntegerType, nullable = true) ::
      StructField("user_location_city", IntegerType, nullable = true) ::
      StructField("orig_destination_distance", DoubleType, nullable = true) ::
      StructField("user_id", IntegerType, nullable = true) ::
      StructField("is_mobile", IntegerType, nullable = true) ::
      StructField("is_package", IntegerType, nullable = true) ::
      StructField("channel", IntegerType, nullable = true) ::
      StructField("srch_ci", DateType, nullable = true) ::
      StructField("srch_co", DateType, nullable = true) ::
      StructField("srch_adults_cnt", IntegerType, nullable = true) ::
      StructField("srch_children_cnt", IntegerType, nullable = true) ::
      StructField("srch_rm_cnt", IntegerType, nullable = true) ::
      StructField("srch_destination_id", IntegerType, nullable = true) ::
      StructField("srch_destination_type_id", IntegerType, nullable = true) ::
      StructField("hotel_id", LongType, nullable = true) ::
      Nil
  )

}
