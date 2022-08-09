package producer.functions

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import system._

object KafkaProducer {
  def main(DF: DataFrame): Unit = {
    val producer = new KafkaProducer[String, String](Properties.propertiesKafka)
    val nmbrRows = DF.count().toInt
    for (i <- 0 until nmbrRows) {
      val currentRow = DF.filter(col("id") === i).collectAsList().toString
      val record = new ProducerRecord(Properties.kafkaTopic, i.toString, currentRow)
      producer.send(record)
      println(i, currentRow)
      if (i % Properties.kafkaProdBatchSize == 0) Thread.sleep(Properties.kafkaProdMessageDelay)
    }
    producer.close()
  }
}
