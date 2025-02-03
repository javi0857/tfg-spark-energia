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


object Comparador {
    

    println("EMPEZAMOS EJECUCIÓN")

    // Crear sesión de Spark
    val spark = {
        SparkSession.builder()
            .appName("ComparadorEficiencia")
            .master("local[*]")
            .getOrCreate() 
    }

    val start = "2025-01-01T00:00" //Tenemos datos de Balance desde 2011-01-01
    val end = "2025-01-31T23:59"
    val interval = "year" //Al ser datos diarios podemos solicitar un año entero con cada llamada a la api
    
    //Llamamos a callApi que se encarga de hacer la llamada a la api, tantas veces como intervalos haya entre las fechas start y end
    val listResponses = callApiBalance(start, end, interval)
    def main(args: Array[String]): Unit = {
    
    //Guardar datos en bruto en JSON y Parquet
    val rawDf = listResponses.map(response => responseToDF(response)(spark)).reduce(_ union _)

    // Guardar en JSON (estructura original)
    rawDf.write.mode("overwrite").json("data/raw/balance_raw.json")

    // Guardar en Parquet (estructura original)
    rawDf.write.mode("overwrite").parquet("data/raw/balance_raw.parquet")



  }
    
def callApiBalance (start: String, end: String, interval: String): Seq[Right[Nothing,String]] = {

        // Doy valor a los parámetros para llamar a la API
        val category = "balance"
        val widget = "balance-electrico"
        val time_trunc = "day"
        val lang = "es"
        val geo_trunc = "electric_system"
        val geo_limit = "ccaa"
        val geo_ids = "13"

        // Creamos secuencia de parejas (String, String) segun las fechas que le hemos dado y el intervalo
        val rangoFechas = buildDateRange(start, end , interval) 

        // Creamos una uri para cada pareja
        val listauris = rangoFechas.map { 
            case (start, end) => createUri(category, widget, start, end, time_trunc, lang)
        } 

        // Imprimir uris generadas
        listauris.foreach(println(_))

        // Registrar el tiempo inicial
        val startTime = System.nanoTime()  

        // Hacemos la llamada a la Api creando Futures para hacerlo de forma concurrente
        val futureResponses = listauris.map { uri =>
            Future {
                getApiData(uri) match {
                case Right(response) => Right(response)
                case Left(error) => throw new Exception(error)
                }
            }
        }

        // Utilizar Future.sequence para esperar a que todos los Futures se completen
        val combinedFuture = Future.sequence(futureResponses)

        
        // Utilizar Await para bloquear hasta que todos los Futures se completen (para no terminar el programa)
        val listResponses = Await.result(combinedFuture, Duration.Inf)
        
        // Registrar el tiempo final y mostrar la duración
        val endTimeConcurrent = System.nanoTime()
        val totalTime = (endTimeConcurrent - startTime) / 1e9 // Convertir a segundos

        println(s"TIEMPO TOTAL DE EJECUCIÓN DE LLAMADA A LA API: $totalTime segundos")

        listResponses //Devolvemos la lista con los responses
    }


}
    
    


