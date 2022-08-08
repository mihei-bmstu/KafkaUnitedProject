import producer.functions.{KafkaProducer, LoadTable}

object BootProducer {
  def main(args: Array[String]): Unit = {
    println("Start Producer")
    KafkaProducer.main(LoadTable.load())
  }
}
