package load.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import system.Properties

object LoadHotelWeather {
  def load(spark: SparkSession): Unit = {
    val DF = spark.read
      .format("parquet")
      .load(Properties.pathHotelWeather)
      .withColumn("wthrDate", col("wthr_date").cast(DateType))
      .drop("wthr_date")
      .withColumnRenamed("year", "wthr_year")
      .withColumnRenamed("month", "wthr_month")
      .withColumnRenamed("day", "wthr_day")
      .withColumnRenamed("wthrDate", "wthr_date")

    DF.write
      .mode("overwrite")
      .jdbc(Properties.urlPG, Properties.tablePGHotelWeather, Properties.propertiesPG)
    println("HotelWeather loaded")
  }
}
