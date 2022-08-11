package producer.functions

import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions._
import org.apache.avro.Schema.Parser
import system._

import java.nio.file.{Files, Paths}

object KafkaProducer {
  def main(DF: DataFrame): Unit = {
    /*val producer = new KafkaProducer[String, String](Properties.propertiesKafka)
    val nmbrRows = DF.count().toInt
    for (i <- 0 until 100) {
      val currentRow = DF.filter(col("id") === i).collectAsList().toString
      val record = new ProducerRecord(Properties.kafkaTopic, i.toString, currentRow)
      producer.send(record)
      //if (i % Properties.kafkaProdBatchSize == 0) Thread.sleep(Properties.kafkaProdMessageDelay)
      println(s"sent message $i")
    }
    producer.close()*/

/////////////////  AVRO ////////////////////////////
    val producer = new KafkaProducer[String, GenericData.Record](Properties.propertiesKafka)
    val schemaParser = new Parser
    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("src/main/resources/trip.avsc")))
    val valueSchemaAvro = schemaParser.parse(jsonFormatSchema)
    for (i <- 1 to 10) {
      val avroRecord = new GenericData.Record(valueSchemaAvro)
      val currentRecords = FetchSQLRecord.findById(i)
      currentRecords.foreach { currentRecord => {
        val fieldsWithValues = classOf[ExpediaRecord].getDeclaredFields.map { f =>
          f.setAccessible(true)
          val res = (f.getName, f.get(currentRecord))
          f.setAccessible(false)
          res
        }.toMap
        for ((k, v) <- fieldsWithValues) {
          avroRecord.put(k, v)
          }
        }
      }
      val record = new ProducerRecord(Properties.kafkaTopic, i.toString, avroRecord)
      producer.send(record)
      val ack = producer.send(record).get()
      println(s"${ack.toString} written to ${ack.partition()}")
    }
    producer.flush()
    producer.close()

    /*val nmbrRows = DF.count().toInt
    for (i <- 0 until nmbrRows) {
      val currentRow = DF.filter(col("id") === i).collectAsList().toString
      val record = new ProducerRecord(Properties.kafkaTopic, i.toString, currentRow)
      producer.send(record)
      if (i % Properties.kafkaProdBatchSize == 0) Thread.sleep(Properties.kafkaProdMessageDelay)
    }
    */

////////////////////  AVRO  END   ///////////////////////////

/*    Logger.getLogger("org").setLevel(Level.ERROR)
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
      tempDF.select(to_avro(struct(tempDF.columns.map(column):_*)).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", Properties.kafkaTopic)
        .save()

      if (i % Properties.kafkaProdBatchSize == 0) Thread.sleep(Properties.kafkaProdMessageDelay)
    }*/
  }
}
