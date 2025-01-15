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

object Consulta1 {
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
            .parquet(filePathMercados)

        val filePathBalance = "data/parquet/dsBalanceNacional11-24.parquet"         
        val datosBalance: DataFrame = spark.read
            .parquet(filePathBalance)
        
    
        datosBalance.show()

        //----------------------------- Fin lectura ----------------------------------------
        import org.apache.spark.sql.expressions.Window


    //Datos diarios 
        val ventana = Window.partitionBy("Fecha")
        val datosBalanceDiarios = datosBalance
            .withColumn("PorcentajeSobreFamilia", round($"Porcentaje" * 100,2) )
            .drop("Porcentaje")
            .withColumn(
                    "TotalGenerado", 
                    round(sum(when(!$"Compuesto" && $"Familia" =!= "Demanda" ,$"Valor").otherwise(0)).over(ventana),2)
            )
            .withColumn("PorcentajeSobreTotal", round($"Valor" / $"TotalGenerado" * 100,2))
            

        

        //Datos mensuales
        val ventanaMensual = Window.partitionBy("FechaMensual")
        val datosBalanceMensuales = datosBalance
            .withColumn("FechaMensual", date_format($"Fecha", "yyyy-MM"))
            .groupBy($"FechaMensual", $"Familia", $"Tipo", $"Compuesto")
            .agg(
                sum("Valor").as("ValorMensual"),
                avg("Porcentaje").as("PorcentajeMensualSobreFamilia")
                )
            .withColumn(
                    "TotalGenerado", 
                    round(sum(when(!$"Compuesto" && $"Familia" =!= "Demanda" ,$"ValorMensual").otherwise(0)).over(ventanaMensual),2)
            )
            .withColumn("PorcentajeSobreTotal", round($"ValorMensual" / $"TotalGenerado" * 100,2))

            
        
        //Datos anuales
        val ventanaAnual = Window.partitionBy("Año")

        val datosBalanceAnuales = datosBalance
            .withColumn("Año", date_format($"Fecha", "yyyy"))
            .groupBy($"Año", $"Familia", $"Tipo", $"Compuesto", $"BajasEmisiones")
            .agg(
                sum("Valor").as("ValorAnual"),
                avg("Porcentaje").as("PorcentajeAnualSobreFamilia")
                )
            .withColumn(
                    "TotalGenerado", 
                    round(
                        sum(
                            when(!$"Compuesto" && $"Familia" =!= "Demanda" ,$"ValorAnual")
                                .otherwise(0)).over(ventanaAnual),2)
            )
            .withColumn(
                    "PorcentajeSobreTotal", 
                    round($"ValorAnual" / $"TotalGenerado" * 100,2))
            .withColumn(
                    "PorcentajeBajasEmisiones", 
                    round(
                        sum(
                            when($"BajasEmisiones" && !$"Compuesto" && $"Familia" =!= "Demanda", $"ValorAnual")
                                .otherwise(0)).over(ventanaAnual) / $"TotalGenerado" * 100,2)
           
            )



        val evolucionRenovablesAnual = datosBalanceAnuales
            .filter($"Tipo".isin("Generación renovable", "Saldo I. internacionales"))
            .select($"Año",$"Tipo", $"PorcentajeSobreTotal", $"TotalGenerado", $"PorcentajeBajasEmisiones")
            .orderBy($"Tipo", $"Año")

    
        evolucionRenovablesAnual.show()

        datosBalanceAnuales.write
            .mode("overwrite")
            .parquet("data/parquet/dsDatosBalanceAnuales.parquet")



        spark.stop()
    
    }

}

