package stream.functions

import org.apache.spark.sql.SparkSession
import system._

case class TreatLineToDF(spark: SparkSession, str: String) {
  def treatLine(): Unit = {
    import spark.implicits._
    val columns = Properties.schemaExpedia.fieldNames
    println("treating line... " + str)
    /*val dfSeq = str.split(",")
      .map(_.replace("[", ""))
      .map(_.replace("]", ""))
      .toSeq
    val rdd = spark.sparkContext.parallelize(dfSeq)
    val df = rdd.toDF()*/
    val cols = Seq("language", "users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)
    val dfFromRDD1 = rdd.toDF()
    dfFromRDD1.printSchema()
  }
}
