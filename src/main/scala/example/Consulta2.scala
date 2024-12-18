package example

import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.concurrent.duration._
import scala.concurrent.Await

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import example.Utils._

object Consulta2 {
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

        //------------------------------Lectura datasets con información ----------------------------------------
        val filePathMercados = "data/parquet/dsMercadoNacionalTotalPrueba.parquet"
        val dfMercados: DataFrame = spark.read
        //.option("header", "true") // Si el archivo tiene un encabezado
        //.option("inferSchema", "true") // Para inferir automáticamente el esquema
        //.csv(filePath)
            .parquet(filePathMercados)

        val filePathBalance = "data/parquet/dsBalanceNacional11-24.parquet"         
        val dfBalance: DataFrame = spark.read
        //.option("header", "true") // Si el archivo tiene un encabezado
        //.option("inferSchema", "true") // Para inferir automáticamente el esquema
        //.csv(filePath)
            .parquet(filePathBalance)
        
        dfMercados.show()
    
        dfBalance.show()

        //----------------------------- Fin lectura ----------------------------------------

        // Calcular el precio promedio diario del mercado
        val mercadoDiario = dfMercados
            .groupBy(date_format(col("Fecha"),"yyyy-MM-dd").as("FechaDia"))
            .agg(avg("Valor_Mercado_Spot").as("ValorMercadoSpot"))
            .withColumn("PrecioMercado", round(col("ValorMercadoSpot"), 2))
            .drop("ValorMercadoSpot")


        //Sacamos las 7 fuentes de energía más relevantes
        val top7Energias = dfBalance
            .filter($"Compuesto" === false)
            .filter($"Familia" =!= "Demanda")
            .groupBy($"Tipo")
            .agg(sum($"Valor").as("ValorTotal"))
            .withColumn("Valor total millones MWh", round(col("ValorTotal") / 1e6, 2))
            .orderBy(desc("Valor total millones MWh"))
            .limit(7)
            .select($"Tipo")
            .as[String]
            .collect
            .toSeq 


        //Filtrar el dataset de balance para incluir solo las fuentes principales
        val dfBalanceFiltrado = dfBalance 
            .filter($"Tipo".isin(top7Energias: _*))
            .withColumn("FechaDia", date_format(col("Fecha"), "yyyy-MM-dd")) // Extraer solo la parte del día

        // // Calcular el porcentaje diario de cada tipo de energía
        val windowTotal = Window.partitionBy("FechaDia") // Ventana para agrupar por día

        val dfBalanceConPorcentajeTotal = dfBalanceFiltrado
            .withColumn("GeneracionTotalDiaria", sum("Valor").over(windowTotal)) // Total de generación por cada día
            .withColumn("PorcentajeRespectoTotal", round((col("Valor") / col("GeneracionTotalDiaria")) * 100, 2)) //Calcular porcentaje
            .select("FechaDia", "Tipo", "Familia", "Valor", "PorcentajeRespectoTotal") //Selectiono las columnas relevantes

        // Pivotar para generar columnas separadas por rank
        val dfBalancePivotadoTipo = dfBalanceConPorcentajeTotal
            .groupBy("FechaDia")
            .pivot("Tipo")
            .agg(first("PorcentajeRespectoTotal").as("%"), first("Valor").as("MWh"))

        //Union de los dos dataframes
        val unionMercadosBalance = mercadoDiario.join(dfBalancePivotadoTipo, Seq("FechaDia"), "inner") 

        unionMercadosBalance.show()

        val dfRenombrado = unionMercadosBalance.columns.foldLeft(unionMercadosBalance)((dfActual, colName) => 
            dfActual.withColumnRenamed(colName, colName.replace(" ", "_"))
        )

        dfRenombrado.write
            .mode("overwrite")
            .option("encoding", "UTF-8")
            //.option("header", "true").csv("data/csv/dsMercadoNacionalTotal.csv")
            .parquet("data/parquet/DatosConsulta2.parquet")

        spark.stop()
    
    }

}
