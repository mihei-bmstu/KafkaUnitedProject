import producer.functions._
import scalikejdbc.{AutoSession, ConnectionPool}
import org.apache.log4j.{Level, Logger}
import system._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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

    val av = Future{ KafkaProducerAvro.main(20) }
    val st = Future{ KafkaProducerString.main(LoadTable.load(Properties.tablePGExpedia), 100000) }
    Await.result(av, Duration.Inf)
    Await.result(st, Duration.Inf)
  }
}
