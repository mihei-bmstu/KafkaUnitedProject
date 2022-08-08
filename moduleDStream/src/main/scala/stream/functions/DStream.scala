package stream.functions

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import system._
import stream.functions._


object DStream extends Serializable {

  def readStream() : Unit = {
    val columns = Properties.schemaExpedia.fieldNames.toSeq

    def treatLine(spark: SparkSession, str: String): Unit = {
      import spark.implicits._
      val columns = Properties.schemaExpedia.fieldNames
      println("treating line... " + str)
      /*val dfSeq = str.split(",")
        .map(_.replace("[", ""))
        .map(_.replace("]", ""))
        .toSeq
      val rdd = spark.sparkContext.parallelize(dfSeq)
      val df = rdd.toDF()*/
      val cols = Seq("language","users_count")
      val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
      val rdd = spark.sparkContext.parallelize(data)
      val dfFromRDD1 = rdd.toDF()
      dfFromRDD1.printSchema()
    }

    def threatRDD(spark: SparkSession, rdd: RDD[String]): DataFrame = {
      import spark.implicits._
      val rddFlat = rdd.flatMap(l => l.split(","))
      //val rddT = spark.sparkContext.parallelize(rddFlat.collect.toSeq.transpose)
      val df = rdd.toDF()
      df.show()
      df
    }

    val spark = SparkSession.builder()
      .appName("DFReader")
      .master("local[2]")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer-group-2"
    )

    val topic = Set(Properties.kafkaTopic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams))

    import spark.implicits._
    val lines = stream.map(_.value)
    lines.foreachRDD(_.foreach(println))
    //lines.foreachRDD(rdd => threatRDD(spark, rdd))
    //lines.foreachRDD(_.foreach(v => treatLine(spark, v)))
    //lines.foreachRDD(rdd => rdd.saveAsTextFile("./rdds/"))

    lines.foreachRDD(rdd => rdd.toDF()
      .write
      .mode("append")
      .jdbc(Properties.urlPG, Properties.tableMessages, Properties.propertiesPG)
      )

    ssc.start()
    ssc.awaitTermination()



  }
}
