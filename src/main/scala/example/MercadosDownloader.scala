package example

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._


import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._

import example.Utils._

object MercadosDownloader {
  def main(args: Array[String]): Unit = {
    
    println("EMPEZAMOS EJECUCIÓN")


    // Doy valor a los parámetros para llamar a la API

        val category = "mercados"
        val widget = "precios-mercados-tiempo-real"
        val time_trunc = "hour"
        val lang = "es"
        val geo_trunc = "electric_system"
        val geo_limit = "ccaa"
        val geo_ids = "13"

        val start = "2014-01-01T00:00"
        val end = "2024-12-31T23:59"
        val interval = "month"

    
    // Crear sesión de Spark
    val spark = SparkSession.builder()
            .appName("MercadosDownloader")
            .master("local[*]")
            .getOrCreate() 
    

    // Creamos secuencia de parejas (String, String) segun las fechas que le hemos dado y el intervalo
    val rangoFechas = buildDateRange(start, end , interval) 

    // Creamos una uri para cada pareja
    val listauris = rangoFechas.map { 
        case (start, end) => createUri(category, widget, start, end, time_trunc, lang)
    } 

    // Imprimir uris generadas
    listauris.foreach(println(_))

    // Registrar el tiempo inicial
    val startTimeConcurrent = System.nanoTime()  

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
    val totalTime = (endTimeConcurrent - startTimeConcurrent) / 1e9 // Convertir a segundos
    println(s"TIEMPO TOTAL DE EJECUCIÓN DE LLAMADA A LA API: $totalTime segundos")

    //Ejecutar si no esta vacio
    if (listResponses.nonEmpty){
        val listModels = listResponses.map{
            response => transformToMercadosModel(responseToDF(response)(spark))(spark)
        }
        val modelMercados = listModels.reduce((df1, df2) => df1.unionByName(df2, allowMissingColumns = true))
        modelMercados.show()
        
        //Escribimos los datos en .csv o .parquet
        modelMercados.write
            .mode("overwrite")
            //.option("header", "true").csv("data/csv/dsMercadoNacionalTotal.csv")
            .parquet("data/parquet/dsMercadoNacionalTotal.parquet")

    } else {
        println("No se obtuvieron respuestas para la API")
    }


    println("FINAL DE LA EJECUCIÓN")

    spark.stop()



  }



  //Transformamos los datos de la API al modelo deseado para Mercado

  def transformToMercadosModel(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

         // Importar implicits para poder usar $"columnName"
        import spark.implicits._

        // Definir los tipos de energía de bajas emisiones
        val transformedDF = df.withColumn("included", explode($"included"))
            .withColumn("TipoMercado", $"included.type")
            .withColumn("Values", explode($"included.attributes.values"))
            .select(
                $"Values.datetime".cast("timestamp").as("FechaAux"),
                $"TipoMercado",
                $"Values.value".as("Valor"),
                $"Values.percentage".as("Porcentaje")
            )
            .withColumn("Fecha", expr("FechaAux + INTERVAL 1 HOUR"))
            .drop($"FechaAux")

        //Agrupar y pivotar el dataframe
        val pivotedDF = transformedDF.groupBy("Fecha")
            .pivot("TipoMercado")
            .agg(
                sum("Valor").as("Valor"),
                sum("Porcentaje").as("Porcentaje")
            )
        
        val renamedDF = pivotedDF
            .withColumnRenamed("PVPC (€/MWh)_Valor", "Valor_PVPC")
            .withColumnRenamed("PVPC (€/MWh)_Porcentaje", "Porcentaje_PVPC")
            .withColumnRenamed("Precio mercado spot_Valor", "Valor_Mercado_Spot")
            .withColumnRenamed("Precio mercado spot_Porcentaje", "Porcentaje_Mercado_Spot")

        
            
        
        // // Añadir columnas si no existen (hasta 2022 no había valor mercado PVPC)
        // val columnasNecesarias = Seq("Valor_PVPC", "Porcentaje_PVPC", "Valor_Mercado_Spot", "Porcentaje_Mercado_Spot")
        // val completeDF = columnasNecesarias.foldLeft(renamedDF) { (df, columna) =>
        //     if (!df.columns.contains(columna)) {
        //         df.withColumn(columna, lit(null).cast("double")) // Añadir columna vacía si no existe
        //     } else {
        //         df
        //     }
        // }

        renamedDF 
    }
}
