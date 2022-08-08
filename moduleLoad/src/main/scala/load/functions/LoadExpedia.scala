package load.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import system._

object LoadExpedia {
  def load(spark: SparkSession): Unit = {

    val DF = spark.read
      .option("dateFormat","yyyy-MM-dd hh:mm:ss")
      .format("avro")
      .load(Properties.pathExpedia)
      .withColumn("dateTime", col("date_time").cast(TimestampType))
      .withColumn("srchCi", col("srch_ci").cast(DateType))
      .withColumn("srchCo", col("srch_co").cast(DateType))
      .drop("date_time", "srch_ci", "srch_co")
      .withColumnRenamed("dateTime", "date_time")
      .withColumnRenamed("srchCi", "srch_ci")
      .withColumnRenamed("srchCo", "srch_co")

    DF.write
      .mode("overwrite")
      .jdbc(Properties.urlPG, Properties.tablePGExpedia, Properties.propertiesPG)
    println("Expedia loaded")
  }

}
