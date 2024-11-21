package example

import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._
import scala.concurrent.duration._
import scala.concurrent.Await

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import example.Utils._


object Main {
    
    def main(args: Array[String]): Unit = {
        
        //Creamos sesión de Spark
        val spark = SparkSession.builder()
            .appName("spark-javi")
            .master("local[*]")
            .config("spark.hadoop.hadoop.security.token.service.use_ip", "true")
            .config("spark.hadoop.io.native.lib.available", "false") 
            .getOrCreate()  

        import spark.implicits._ 


        val rangoFechas = buildDateRange("2014-01-01T00:00","2024-10-31T23:59","month")  

        

        //Doy valor a los parámetros para llamar a la API

        val category = "generacion"
        val widget = "estructura-generacion"
        val time_trunc = "month" 
        val lang = "es"
        val geo_trunc = "electric_system"
        val geo_limit = "ccaa"
        val geo_ids = "13"


      


        //val listauris = rangoFechas.map { case (start, end) => createUri(category, widget, start, end, time_trunc, lang) }
             
        //listauris.foreach(println)

        // val startTimeSequential = System.nanoTime()  // Registrar el tiempo inicial

        // val listaResponse = listauris.map(getApiData(_))  // Ejecutar las llamadas a la API

        // val endTimeSequential = System.nanoTime()  // Registrar el tiempo final

        // val elapsedTimeSequential = (endTimeSequential - startTimeSequential) / 1e9  // Convertir a segundos

        // println(s"Tiempo de ejecución secuencial: $elapsedTimeSequential segundos")


        val startTimeConcurrent = System.nanoTime()  // Registrar el tiempo inicial

        // Crear los Futures para cada URI
        // val futureResponses = listaUris.map { uri =>
        //     Future {
        //         getApiData(uri) match {
        //         case Right(response) => response
        //         case Left(error) => throw new Exception(error)
        //         }
        //     }
        // }

        // Utilizar Future.sequence para esperar a que todos los Futures se completen
        // val combinedFuture = Future.sequence(futureResponses)

        // // Registrar el tiempo final una vez que todos los Futures se completen
        // combinedFuture.onComplete {
        //     case Success(_) => 
        //         val endTimeConcurrent = System.nanoTime()
        //         val elapsedTimeConcurrent = (endTimeConcurrent - startTimeConcurrent) / 1e9  // Convertir a segundos
        //         println(s"Tiempo de ejecución concurrente: $elapsedTimeConcurrent segundos")
            
        //     case Failure(e) =>
        //         println(s"Error en alguna de las descargas: ${e.getMessage}")
        // }

        // Utilizar Await para bloquear hasta que todos los Futures se completen (para no terminar el programa)
        // Await.result(combinedFuture, Duration.Inf)

        // println("FINAL DE LA EJECUCIÓN")
        // val listModelsMercados = listaResponse.map {
        //     json => transformToMercadosModel(responseToDF(json)(spark))(spark)
        // }
        // val modelMercados = listModelsMercados.reduce(_ union _)

        // modelMercados.orderBy("Fecha")

        // modelMercados.coalesce(1).write
        //     .mode("overwrite")
        //     .option("header", "true")
        //     .csv("data/datasetMercadoNacional-14-24")


    
        //spark.stop()
    
    }


}