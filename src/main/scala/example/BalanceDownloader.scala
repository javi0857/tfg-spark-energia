package example

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._


import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._


import example.Utils._
import cats.instances.boolean


object BalanceDownloader {
  def main(args: Array[String]): Unit = {
    
    println("EMPEZAMOS EJECUCIÓN")

    // Crear sesión de Spark
    val spark = {
        SparkSession.builder()
            .appName("BalanceDownloader")
            .master("local[*]")
            .getOrCreate() 
    }

    import org.apache.spark.sql.functions._
    val start = "2011-01-01T00:00" //Tenemos datos de Balance desde 2011-01-01
    val end = "2025-01-31T23:59"
    val interval = "year" //Al ser datos diarios podemos solicitar un año entero con cada llamada a la api
    
    //Llamamos a callApi que se encarga de hacer la llamada a la api, tantas veces como intervalos haya entre las fechas start y end
    val listResponses = callApiBalance(start, end, interval)
    

    //Ejecutar si no esta vacio
    if (listResponses.nonEmpty){


        val listModels = listResponses.map{
            response => transformToBalanceModel(responseToDF(response)(spark))(spark)
        }
        val model = listModels.reduce(_ union _)
        
    

        //Imprimimos primeras lineas del modelo
        model.show() 

        //Escribimos los datos en .csv y .parquet
        model.write
            .mode("overwrite")
            .option("header", "true")
            .csv("data/csv/dsBalanceNacionalAnalisis.csv")
        

        model.write
            .mode("overwrite")
            .partitionBy("Año", "Mes") 
            .parquet("data/parquet/dsBalanceNacionalParticionado.parquet")


    } else {
        println("No se obtuvieron respuestas para la API")
    }


    println("FINAL DE LA EJECUCIÓN")

    spark.stop()
  }



    //Definición funciones necesarias: 


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

  def transformToBalanceModel(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

        // Importar implicits para poder usar $"columnName"
        import spark.implicits._

        // Definir los tipos de energía de bajas emisiones
        val bajasEmisiones = Seq(
            "Solar fotovoltaica", 
            "Solar térmica", 
            "Nuclear", 
            "Hidroeólica", 
            "Eólica", 
            "Generación renovable", 
            "Otras renovables",
            "Hidráulica", 
            "Residuos renovables"

        )

        //Crear modelo desanidado
        df.withColumn("FamilyGroup", explode($"included"))
            .withColumn("TypeGroup", explode($"FamilyGroup.attributes.content"))
            .withColumn("Values", explode($"TypeGroup.attributes.values"))
            .select(
                $"FamilyGroup.type".as("Familia"),
                $"TypeGroup.type".as("Tipo"),
                $"TypeGroup.attributes.composite".as("Compuesto"),
                $"Values.datetime".cast("timestamp").as("FechaAux"), 
                $"Values.percentage".as("Porcentaje"),
                $"Values.value".as("Valor")
            )
            .withColumn("Fecha", expr("FechaAux + INTERVAL 1 HOUR"))
            .withColumn("BajasEmisiones", $"Tipo".isin(bajasEmisiones: _*))
            .withColumn("Año", year($"Fecha"))
            .withColumn("Mes", month($"Fecha"))
            .drop($"FechaAux")
    }
}