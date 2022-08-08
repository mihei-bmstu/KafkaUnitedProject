package producer.functions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import system.Properties

object LoadTable {
  def load(): DataFrame = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("StudyProjectKafka")
      .getOrCreate()

    val DF = spark.read.jdbc(
      Properties.urlPG,
      Properties.tablePGExpedia,
      Properties.propertiesPG)

    DF
  }

}
