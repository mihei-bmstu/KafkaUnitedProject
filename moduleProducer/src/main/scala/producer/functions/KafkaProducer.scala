package producer.functions

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import system._

object KafkaProducer {
  def main(DF: DataFrame): Unit = {
    val producer = new KafkaProducer[String, String](Properties.propertiesKafka)
    val nmbrRows = DF.count()
    var i: Int = 0
    while (i < nmbrRows) {
      val currentRow = DF.filter(col("id") === i).collectAsList().toString
      val record = new ProducerRecord(Properties.kafkaTopic, i.toString, currentRow)
      producer.send(record)
      println(i, currentRow)
      i += 1
      if (i % 1 == 0) Thread.sleep(10000)
    }
    producer.close()

  }

}
