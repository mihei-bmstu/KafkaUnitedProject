import producer.functions.KafkaProducer
import system._

object BootProducer {
  def main(args: Array[String]): Unit = {
    println("Start Producer")
    KafkaProducer.main(LoadTable.load(Properties.tablePGExpedia))
  }
}
