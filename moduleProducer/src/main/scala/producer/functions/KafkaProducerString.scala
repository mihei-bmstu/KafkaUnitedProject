package producer.functions

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import system._

object KafkaProducerString {
  def main(DF: DataFrame): Unit = {
    val producer = new KafkaProducer[String, String](Properties.propertiesKafkaString)
    val nmbrRows = DF.count().toInt
    for (i <- 0 until nmbrRows) {
      val currentRow = DF.filter(col("id") === i).collectAsList().toString
      val record = new ProducerRecord(Properties.kafkaTopicString, i.toString, currentRow)
      val ack = producer.send(record).get()
      println(s"${ack.toString} written to ${ack.partition()}")
      if (i % Properties.kafkaProdBatchSize == 0) Thread.sleep(Properties.kafkaProdMessageDelay)
    }
    producer.close()

  }
}
