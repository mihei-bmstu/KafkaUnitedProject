import producer.functions._
import scalikejdbc.{AutoSession, ConnectionPool}
import org.apache.log4j.{Level, Logger}
import system._

object BootProducer {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)
    Logger.getLogger("io").setLevel(Level.ERROR)
    Logger.getLogger("ch").setLevel(Level.ERROR)
    println("Start Producer")
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(Properties.urlPG, Properties.userPG, Properties.passPG)

    implicit val session: AutoSession.type = AutoSession
    //println(FetchSQLRecord.findById(11))

    KafkaProducer.main(LoadTable.load(Properties.tablePGExpedia))
  }
}
