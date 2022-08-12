package producer.functions

import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.avro.Schema.Parser
import system._

import java.nio.file.{Files, Paths}

object KafkaProducerAvro {
  def main(nmbrMessages: Int = 1000): Unit = {
    val producer = new KafkaProducer[String, GenericData.Record](Properties.propertiesKafkaAvro)
    val schemaParser = new Parser
    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("src/main/resources/trip.avsc")))
    val valueSchemaAvro = schemaParser.parse(jsonFormatSchema)

    for (i <- 1 to nmbrMessages) {
      val avroRecord = new GenericData.Record(valueSchemaAvro)
      val currentRecords = FetchSQLRecord.findById(i)
      currentRecords.foreach {
        currentRecord => {
        val fieldsWithValues = classOf[ExpediaRecord].getDeclaredFields.map { f =>
          f.setAccessible(true)
          val res = (f.getName, f.get(currentRecord))
          f.setAccessible(false)
          res
        }.toMap
        for ((k, v) <- fieldsWithValues) { avroRecord.put(k, v) }
        }
      }
      val record = new ProducerRecord(Properties.kafkaTopicAvro, i.toString, avroRecord)
      val ack = producer.send(record).get()
      println(s"${ack.toString} written to ${ack.partition()}")
      if (i % Properties.kafkaProdBatchSize == 0) Thread.sleep(Properties.kafkaProdMessageDelay)
    }
    producer.flush()
    producer.close()
  }
}
