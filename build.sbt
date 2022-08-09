ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

val sparkVersion = "3.1.0"
val core = "org.apache.spark" %% "spark-core" % sparkVersion
val sql = "org.apache.spark" %% "spark-sql" % sparkVersion
val avro = "org.apache.spark" %% "spark-avro" % sparkVersion
val sparkStream = "org.apache.spark" %% "spark-streaming" % sparkVersion
val streamKafka = "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
val kafka = "org.apache.kafka" % "kafka-clients" % sparkVersion
val pgDriver = "org.postgresql" % "postgresql" % "42.3.6"
val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion
val sparkSQLkafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

lazy val root = (project in file("."))
  .settings(
    name := "KafkaProjectUnited",
    libraryDependencies := Seq(core, sql)
  )

lazy val load = (project in file("./moduleLoad"))
  .settings(
    name := "LoadTables",
    libraryDependencies ++= Seq(avro, pgDriver)
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
      sparkAvro,
      sparkSQLkafka)
  )
  .dependsOn(root)

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
