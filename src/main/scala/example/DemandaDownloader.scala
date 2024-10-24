package example

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._


import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._

import example.Utils._

object DemandaDownloader {
  def main(args: Array[String]): Unit = {
    println("EMPEZAMOS EJECUCIÓN")


    // Doy valor a los parámetros para llamar a la API
        val category = "demanda"
        val widget = "demanda-tiempo-real"
        val time_trunc = "hour"
        val lang = "es"

        val geo_trunc = "electric_system"
        val geo_limit = "ccaa"
        val geo_ids = "13"

        val start = "2014-01-01T00:00" //Tenemos datos de Demanda desde 2014-01-01
        val end = "2024-12-31T23:59"
        val interval = "month" //Al ser datos a tiempo real podemos solicitar un intervalo de un mes con cada llamada a la api

    
    // Crear sesión de Spark
    val spark = {
        SparkSession.builder()
            .appName("MercadosDownloader")
            .master("local[*]")
            .getOrCreate() 
    }

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

    //Ejecutar si no esta vacio
    if (listResponses.nonEmpty){
        val listModels = listResponses.map{
            response => transformToDemandaDataframe(responseToDF(response)(spark))(spark)
        }
        val model = listModels.reduce(_ union _)
        
        //Imprimimos primeras lineas del modelo
        model.show() 

        model.write
            .mode("overwrite")
            .option("header", "true")
            .csv("data/dsDemandaNacionalTotal.csv")
    } else {
        println("No se obtuvieron respuestas para la API")
    }


    println("FINAL DE LA EJECUCIÓN")

    spark.stop()
  }


  def transformToDemandaDataframe(dFrameDemanda: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // Importar implicits para poder usar $"columnName"
    import spark.implicits._

    // Definir los tipos de energía de bajas emisiones
    val transformedDfDemanda = dFrameDemanda
        .withColumn("included", explode($"included"))
        .withColumn("TipoDemanda", $"included.type")
        .withColumn("Values", explode($"included.attributes.values"))
        .select(
            $"Values.datetime".cast("timestamp").as("Fecha"),
            $"TipoDemanda",
            $"Values.value".as("Valor"),
            $"Values.percentage".as("Porcentaje")
        )
    
    val pivotedDfDemanda = transformedDfDemanda     
        .groupBy("Fecha")
        .pivot("TipoDemanda")
        .agg(
            first("Valor").as("Valor"),
            first("Porcentaje").as("Porcentaje")
        )
        .withColumnRenamed("Demanda prevista_Valor", "Valor_previsto")
        .withColumnRenamed("Demanda prevista_Porcentaje", "Porcentaje_previsto")
        .withColumnRenamed("Demanda programada_Valor", "Valor_programado")
        .withColumnRenamed("Demanda programada_Porcentaje", "Porcentaje_programado")
        .withColumnRenamed("Demanda real_Valor", "Valor_real")
        .withColumnRenamed("Demanda real_Porcentaje", "Porcentaje_real")
    
    pivotedDfDemanda
  }

}
