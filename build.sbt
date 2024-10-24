import Dependencies._

ThisBuild / scalaVersion     := "2.13.15"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "tfg.javi"
ThisBuild / organizationName := "javi"

lazy val root = (project in file("."))
  .settings(
    name := "spark-datos-energia",
  )

val sparkVersion = "3.5.0"
libraryDependencies ++= Seq(
  //Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-common" % "3.3.6",  // Asegúrate de usar una versión reciente de Hadoop
  "org.apache.hadoop" % "hadoop-client" % "3.3.6",

  //Test
  Dependencies.munit,

  // Llamada API REST con STTP 4
  "com.softwaremill.sttp.client4" %% "core" % "4.0.0-M19",
  "com.softwaremill.sttp.client4" %% "async-http-client-backend-future" % "4.0.0-M19",  // Backend asíncrono
  "com.softwaremill.sttp.client4" %% "circe" % "4.0.0-M19",  // STTP con soporte para Circe (JSON)

  //Trabajaar con JSON
  "io.circe" %% "circe-generic" % "0.14.1",  // Circe para trabajar con JSON
  "io.circe" %% "circe-parser" % "0.14.1"    // Circe para parsear JSON

)
