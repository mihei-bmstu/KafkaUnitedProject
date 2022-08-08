import org.apache.log4j.{Level, Logger}
import stream.functions._

object BootDStream {
  def main(args: Array[String]): Unit = {
    println("Start DStream")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)
    DStream.readStream()
  }
}
