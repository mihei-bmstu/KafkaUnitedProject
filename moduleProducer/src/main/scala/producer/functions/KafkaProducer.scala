package producer.functions

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions._
import system._

import java.nio.file.{Files, Paths}

object KafkaProducer {
  def main(DF: DataFrame): Unit = {
/*    val producer = new KafkaProducer[String, String](Properties.propertiesKafka)
    val nmbrRows = DF.count().toInt
    for (i <- 0 until nmbrRows) {
      val currentRow = DF.filter(col("id") === i).collectAsList().toString
      val record = new ProducerRecord(Properties.kafkaTopic, i.toString, currentRow)
      producer.send(record)
      println(i, currentRow)
      if (i % Properties.kafkaProdBatchSize == 0) Thread.sleep(Properties.kafkaProdMessageDelay)
    }
    producer.close()*/
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("StudyProjectKafka")
      .getOrCreate()
    import spark.implicits._
    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("src/main/resources/trip.avsc")))
    val nmbrRows = DF.count().toInt
    for (i <- 0 until nmbrRows) {
      val currentRow = Seq(DF.filter(col("id") === i).collectAsList().toString)
      val tempDF = spark.sparkContext.parallelize(currentRow).toDF("value")
      tempDF.select(to_avro(col("value")).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", Properties.kafkaTopic)
        .save()

      if (i % Properties.kafkaProdBatchSize == 0) Thread.sleep(Properties.kafkaProdMessageDelay)
    }
  }
}
