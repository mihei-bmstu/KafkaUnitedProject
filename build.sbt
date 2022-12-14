ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

val sparkVersion = "3.1.0"
val core = "org.apache.spark" %% "spark-core" % sparkVersion
val sql = "org.apache.spark" %% "spark-sql" % sparkVersion
val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion
val sparkStream = "org.apache.spark" %% "spark-streaming" % sparkVersion
val streamKafka = "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
val kafka = "org.apache.kafka" % "kafka-clients" % sparkVersion
val pgDriver = "org.postgresql" % "postgresql" % "42.3.6"
val sparkSQLkafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
val scalike = "org.scalikejdbc" %% "scalikejdbc" % "3.5.0"
val abris = "za.co.absa" %% "abris" % "5.1.1"
val jackson = "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.3"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

lazy val root = (project in file("."))
  .settings(
    name := "KafkaProjectUnited",
    libraryDependencies := Seq(core, sql, scalike)
  )

lazy val load = (project in file("./moduleLoad"))
  .settings(
    name := "LoadTables",
    libraryDependencies ++= Seq(sparkAvro, pgDriver)
  )
  .dependsOn(root)

lazy val producer = (project in file("./moduleProducer"))
  .settings(
    name := "KafkaProducer",
    libraryDependencies ++= Seq(kafka,
      pgDriver,
      "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
      "org.apache.logging.log4j" % "log4j-core" % "2.17.2",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.2",
      "io.confluent" % "kafka-avro-serializer" % "3.2.1",
      sparkAvro,
      sparkSQLkafka)
  )
  .dependsOn(root)
//      "ch.qos.logback"  %  "logback-classic"   % "1.2.3",

lazy val dstream = (project in file("./moduleDStream"))
  .settings(
    name := "DStream",
    libraryDependencies ++= Seq(sparkStream, streamKafka, pgDriver, sparkAvro)
  )
  .dependsOn(root)

lazy val treatMessage = (project in file("./moduleTreatMessage"))
  .settings(
    name := "TreatMessage",
    libraryDependencies ++= Seq(pgDriver)
  )
  .dependsOn(root)

lazy val avroConsumer = (project in file("./moduleAvroConsumer"))
  .settings(
    name := "avroConsumer",
    libraryDependencies ++= Seq(abris, sparkSQLkafka, sparkAvro, jackson, kafka, pgDriver)
  )
  .dependsOn(root)
