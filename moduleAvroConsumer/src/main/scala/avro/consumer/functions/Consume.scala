package avro.consumer.functions

import org.apache.spark.sql.SparkSession
import system.{LoadTable, Properties}
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.abris.avro.functions.from_avro
import org.apache.spark.sql.functions._

object Consume {
  def main(): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Consumer")
      .config("checkpointLocation", "./checkpoint")
      .getOrCreate()

    import spark.implicits._

    val DFWeather = LoadTable.load(Properties.tablePGHotelWeather)

    val abrisConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(Properties.kafkaTopicAvro)
      .usingSchemaRegistry(Properties.shemaRegistry)

    val df = spark.readStream
      .format("kafka")
      .options(
        Map(
        "kafka.bootstrap.servers" -> Properties.kafkaHost,
        "subscribe" -> Properties.kafkaTopicAvro,
        "startingOffsets" -> "earliest")
      )
      .load()

    val readDF = df.select(from_avro('value, abrisConfig)
      .as("row"))
      .select("row.*")

    val joinedDF = readDF.join(DFWeather, readDF("hotel_id") === DFWeather("id") &&
      readDF("srch_ci") === DFWeather("wthr_date"))

    val DFhot = joinedDF.filter('avg_tmpr_c >= Properties.tempLimit)
    val DFcold = joinedDF.filter('avg_tmpr_c < Properties.tempLimit)

    DFhot.groupBy("hotel_id")
      .agg(avg("orig_destination_distance").as("avg_hot"))
      .orderBy('hotel_id)
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .start()

    DFcold.groupBy("hotel_id")
      .agg(avg("orig_destination_distance").as("avg_cold"))
      .orderBy('hotel_id)
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

}
