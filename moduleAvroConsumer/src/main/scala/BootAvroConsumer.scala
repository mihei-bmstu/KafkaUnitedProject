import org.apache.log4j.{Level, Logger}
import avro.consumer.functions.Consume

object BootAvroConsumer {
  def main(args: Array[String]): Unit = {
    println("Start Avro Consumer")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)
    Logger.getLogger("io").setLevel(Level.ERROR)
    Logger.getLogger("ch").setLevel(Level.ERROR)

    Consume.main()

  }

}
