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
        import org.apache.spark.sql.expressions.Window

        

        
            // Filtrar los datos para excluir ciertos valores
        val dfBalanceFiltrado = dfBalance
        .filter($"Compuesto" === false)
        .filter($"Familia" =!= "Demanda")
        .withColumn("FechaDia", date_format(col("FechaCompleta"), "yyyy-MM-dd")) // Extraer solo la parte del día

        // Calcular el porcentaje de cada tipo de energía respecto al total diario
        val windowTotal = Window.partitionBy("FechaDia")

        val dfBalanceConPorcentajeTotal = dfBalanceFiltrado
        .withColumn("GeneracionTotalDiaria", sum("Valor").over(windowTotal)) // Total de generación por cada día
        .withColumn("PorcentajeRespectoTotal", round((col("Valor") / col("GeneracionTotalDiaria")) * 100, 2))
        .select("Familia", "Tipo", "FechaCompleta", "FechaDia", "Valor", "PorcentajeRespectoTotal")

        // Calcular el ranking por porcentaje
        val windowRanking = Window.partitionBy("FechaDia").orderBy(desc("PorcentajeRespectoTotal"))

        val dfRankeado = dfBalanceConPorcentajeTotal
        .withColumn("rank", row_number().over(windowRanking))
        .filter($"rank" <= 3) // Filtrar solo los primeros 3 valores
        .orderBy("FechaCompleta", "rank")

        // Pivotar para generar columnas separadas por rank
        val dfPivotadoTipo = dfRankeado
            .groupBy("FechaCompleta")
            .pivot("rank")
            .agg(first("Tipo").as("Tipo"))

        val dfPivotadoPorcentaje = dfRankeado
            .groupBy("FechaCompleta")
            .pivot("rank")
            .agg(first("PorcentajeRespectoTotal").as("Porcentaje"))

        val dfFinal = dfPivotadoTipo
            .join(dfPivotadoPorcentaje, "FechaCompleta")

      




    
        //spark.stop()
    
    }


}