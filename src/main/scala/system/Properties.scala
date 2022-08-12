package system

import org.apache.spark.sql.types._
import java.util.Properties

object Properties {
  val userPG = "user"
  val passPG = "user"
  val urlPG = "jdbc:postgresql://localhost:5432/demo"
  val pathExpedia = "C:\\Users\\mvchernov\\work\\201source\\07\\expedia\\"
  val pathHotelWeather = "C:\\Users\\mvchernov\\work\\201source\\07\\hotel-weather\\"
  val tablePGExpedia = "expedia"
  val tablePGHotelWeather = "hotel_weather"
  val tableMessages = "kafka_messages"

  val propertiesPG = new Properties()
  propertiesPG.setProperty("user", userPG)
  propertiesPG.setProperty("password", passPG)
  propertiesPG.setProperty("driver", "org.postgresql.Driver")

  val propertiesKafkaAvro = new Properties()
  propertiesKafkaAvro.put("bootstrap.servers", "localhost:9092")
  propertiesKafkaAvro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  propertiesKafkaAvro.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
  propertiesKafkaAvro.put("schema.registry.url","http://localhost:8081")

  val propertiesKafkaString = new Properties()
  propertiesKafkaString.put("bootstrap.servers", "localhost:9092")
  propertiesKafkaString.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  propertiesKafkaString.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val kafkaTopicAvro = "topicAvro"
  val kafkaTopicString = "topicString"
  val kafkaProdBatchSize = 1
  val kafkaProdMessageDelay = 100 //millisecond
  val tempLimit = 20

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
