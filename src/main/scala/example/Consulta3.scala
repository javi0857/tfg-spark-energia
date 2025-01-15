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

object Consulta3 {
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
        val filePathRegiones = "data/parquet/dsBalanceNacionalPorRegionesMensual11-24.parquet"

        val dfRegiones = spark.read
            .parquet(filePathRegiones)
    
        dfRegiones.show()

        //----------------------------- Fin lectura ----------------------------------------


        
        // Definir el año a filtrar
        val año = "2024"

        // Aplicar los filtros iniciales
        val dfFiltrado = dfRegiones
            .filter($"Compuesto" === false) 
            .filter($"Familia" =!= "Demanda")
            .filter(date_format(col("Fecha"), "yyyy") === año)

        // Agrupar por "Region" y "Tipo", y calcular la suma de "Valor" para cada tipo de energía en cada región
        val sumaEnergiaPorRegionYTipo = dfFiltrado
            .groupBy($"Region", $"Tipo")
            .agg(sum("Valor").as("ValorTotal"))

        // Definir una ventana para obtener el tipo de energía con la mayor producción para cada región
        val windowSpec = Window.partitionBy("Region").orderBy(desc("ValorTotal"))

        // Añadir columna de ranking y seleccionar el top 1 por región
        val topEnergiaPorRegion = sumaEnergiaPorRegionYTipo
            .withColumn("rank", row_number().over(windowSpec))
            .filter($"rank" === 1)
            .drop("rank")

        // Calcular la suma de la energía total por cada región
        val sumaEnergiaPorRegion = sumaEnergiaPorRegionYTipo
            .groupBy($"Region")
            .agg(sum("ValorTotal").as("ValorTotalPorRegion"))
            .withColumn("Valor total Region (millones MWh)", round(col("ValorTotalPorRegion") / 1e6, 2))

        // Hacemos un join para combinar los datos de la energía total y la fuente predominante
        val energiaTotalConTopEnergia = sumaEnergiaPorRegion
            .join(topEnergiaPorRegion, Seq("Region"), "inner") // Unimos ambos DataFrames por la columna "Region"
            .select(
                col("Region"),
                col("Valor total Region (millones MWh)").as("Energía_Total_por_Region_millones_MWh"),
                col("Tipo").as("Fuente_de_Energía_Predominante"),
                round(col("ValorTotal") / 1e6, 2).as("Energía_de_Fuente_Predominante_millones_MWh")
            )
            .orderBy(desc("Energía_Total_por_Region_millones_MWh"))

        // Mostrar el resultado
        energiaTotalConTopEnergia.show()


        energiaTotalConTopEnergia.write
            .mode("overwrite")
            .parquet("data/parquet/DatosConsulta3.parquet")

        spark.stop()
    
    }

}