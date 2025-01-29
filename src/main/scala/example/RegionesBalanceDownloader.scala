package example

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._


import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._

import example.Utils._
import cats.instances.boolean
import org.apache.spark.sql.Dataset


//Defino la clase RegionData para crear el dataset con los id de las regiones
case class RegionData (
            region: String, 
            geo_limit: String,
            geo_id: String
        )

object RegionsBalanceDownoloader {
    def main(args: Array[String]): Unit = {
    
        println("EMPEZAMOS EJECUCIÓN")

        // Crear sesión de Spark
        val spark = SparkSession.builder()
                .appName("BalanceDownloader")
                .master("local[*]")
                .getOrCreate() 

        import spark.implicits._

        //Leemos información con los id de las regiones

        val filePath = "data/csv/csvRegionesParaDescargar.csv"         
        val dsRegion: Dataset[RegionData] = spark.read
            .option("header", "true") // Si el archivo tiene un encabezado
            .option("inferSchema", "true") // Para inferir automáticamente el esquema
            .csv(filePath)
            .as[RegionData]


        val start = "2011-01-01T00:00" //Tenemos datos de Balance desde 2011-01-01
        val end = "2024-12-31T23:59"
        val interval = "year" //Al ser datos diarios podemos solicitar un año entero con cada llamada a la api

        //Recorremos la lista de regiones y creamos una dupla (Region, listResponse) por cada fila

        val listRegiones = dsRegion.collect()

        val listDataFramesPorRegion = listRegiones.map { aux => 

            val region = aux.region //Guardamos la región sobre la que vamos a operar
            //Llamada a la API
            val listResponses = callApiBalance(start, end, interval, aux.geo_limit, aux.geo_id)
            
            //Si la api tiene respuesta, hacemos las transformaciones
            if (listResponses.nonEmpty){
                //Transformar las respuestas en un Dataframe
                val listModel = listResponses.map {
                        response => transformToBalanceModel(responseToDF(response)(spark))(spark)
                    }
                val model = listModel.reduce(_ union _)
                    //Añado la columna region al dataframe
                model.withColumn("Region", lit(region))

            } else {
                println("No se obtuvieron respuestas para la API")
                null
            }
            
        } 

        val validModels = listDataFramesPorRegion.filter(_ != null)
        val AllRegionDataFrame = validModels.reduce(_ union _)
        
        AllRegionDataFrame.show()

        //Escribimos los datos en .csv o .parquet
        AllRegionDataFrame.write
            .mode("overwrite")
            //.option("header", "true").csv("data/csv/dsBalanceNacional11-24.csv")
            .parquet("data/parquet/dsBalanceNacionalPorRegionesMensual11-24.parquet")

        spark.stop()

        println("FINAL DE LA EJECUCIÓN")


    }

    

   def callApiBalance (start: String, end: String, interval: String, geoLimit: String, geoIds: String): Seq[Right[Nothing,String]] = {

        // Doy valor a los parámetros para llamar a la API
        val category = "balance"
        val widget = "balance-electrico"
        val time_trunc = "month"
        val lang = "es"
        val geo_trunc = "electric_system"
        val geo_limit = geoLimit
        val geo_ids = geoIds

        // Creamos secuencia de parejas (String, String) segun las fechas que le hemos dado y el intervalo
        val rangoFechas = buildDateRange(start, end , interval) 

        // Creamos una uri para cada pareja
        val listauris = rangoFechas.map { 
            case (start, end) => createUri(category, widget, start, end, time_trunc, lang, geo_trunc, geo_limit, geo_ids)
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
            .drop($"FechaAux")
            .withColumn("BajasEmisiones", $"Tipo".isin(bajasEmisiones: _*))
    }
}