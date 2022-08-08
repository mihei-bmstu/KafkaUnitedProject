import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import load.functions.{LoadExpedia, LoadHotelWeather}

object BootLoad {
  def main(args: Array[String]): Unit = {
    println("Start module Load")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("StudyProjectKafka")
      .getOrCreate()

    LoadExpedia.load(spark)
    LoadHotelWeather.load(spark)
  }
}
