package example


import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._

import sttp.client4._

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
 

object Utils {

  // Funciones comunes
 
  //Crear rango
    def buildDateRange(startDate: String, endDate: String, interval: String): Seq[(String, String)] = {
        // Formato que incluye tanto la fecha como la hora
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm")

        // Parsear las fechas como LocalDateTime
        var start = LocalDateTime.parse(startDate, formatter)
        var end = LocalDateTime.parse(endDate, formatter)
        val now = LocalDateTime.now()
        
        // Asegurarse de que el final no sea posterior a la fecha y hora actuales
        if (end.isAfter(now)) { end = now }

        var ranges = Seq.empty[(String, String)]

        while (!start.isAfter(end)) {
            val first = start
            var last = start

            // Match para incrementar el intervalo de tiempo
            last = interval match {
                case "month" => last.plusMonths(1).minusMinutes(1)  // Incluir hasta el último minuto del mes
                case "year"  => last.plusYears(1).minusMinutes(1)   // Incluir hasta el último minuto del año
                case _       => throw new IllegalArgumentException("Intervalo no válido, Usa 'month' o 'year'")
            }

            // Si `last` es posterior a `end`, ajustarlo a `end`
            if (last.isAfter(end)) { last = end }

            // Agregar el rango a la secuencia
            ranges :+= (first.format(formatter), last.format(formatter))

            // Avanzar `start` al siguiente minuto del intervalo
            start = last.plusMinutes(1)
        }

        ranges
    }


  //Crear URL
    def createUri(category: String, widget: String, start_date: String, end_date: String, time_trunc: String, lang: String, 
                geo_trunc: String = "", geo_limit: String = "", geo_ids: String = ""): String = {
        
        val baseUrl = s"https://apidatos.ree.es/$lang/datos/$category/$widget"
    
        // Crear un mapa con los parámetros obligatorios y opcionales
        val params = Map(
            "start_date" -> start_date,
            "end_date" -> end_date,
            "time_trunc" -> time_trunc,
            "geo_trunc" -> geo_trunc,
            "geo_limit" -> geo_limit,
            "geo_ids" -> geo_ids
        ).filter { case (_, v) => v.nonEmpty } // Filtrar los parámetros que no están vacíos

        //Especifico el orden concreto de los paramtros ya que, Scala map no te garantiza el orden
        val orderedKeys = Seq("start_date", "end_date", "time_trunc", "geo_trunc", "geo_limit", "geo_ids")

        // Unir los parámetros en una cadena
        val queryString = orderedKeys.flatMap(key => params.get(key).map(value => s"$key=$value")).mkString("&")
                    
        //Devuelvo
        s"$baseUrl?$queryString"
    }

    def getApiData(apiUrl: String): Either[String, String] = {
        val request = basicRequest.get(uri"$apiUrl")

        val backend = DefaultSyncBackend()
        val response = request.send(backend)

        response.body match {
            case Right(body) =>
                Right(body)
            case Left(error) =>
                Left(s"Error fetching data: $error")
        }
    }

    
    // Transformar response a DataFrame
    def responseToDF(response: Either[String, String])(implicit spark: SparkSession): DataFrame = {
        response match {
        case Right(body) => 
            val rdd = spark.sparkContext.parallelize(Seq(body))
            spark.read.json(rdd)
        case Left(error) => 
            println(error)
            spark.emptyDataFrame
        }
    }


}