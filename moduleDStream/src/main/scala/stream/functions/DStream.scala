package stream.functions

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import system._

object DStream extends Serializable {

  def readStream() : Unit = {
    val spark = SparkSession.builder()
      .appName("DFReader")
      .master("local[2]")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer-group-2"
    )

    val topic = Set(Properties.kafkaTopicString)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams))

    import spark.implicits._
    val lines = stream.map(_.value)
    lines.foreachRDD(_.foreach(println))

    lines.foreachRDD(rdd => rdd.toDF()
      .write
      .mode("append")
      .jdbc(Properties.urlPG, Properties.tableMessages, Properties.propertiesPG)
      )

    ssc.start()
    ssc.awaitTermination()

  }
}
