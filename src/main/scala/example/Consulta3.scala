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


        spark.stop()
    
    }

}