package system

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadTable {
  def load(tableName: String): DataFrame = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("StudyProjectKafka")
      .getOrCreate()

    val DF = spark.read.jdbc(
      Properties.urlPG,
      tableName,
      Properties.propertiesPG)

    DF
  }
}
