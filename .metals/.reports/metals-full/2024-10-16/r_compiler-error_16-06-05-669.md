file:///C:/Proyectos/spark-datos-energia/src/main/scala/example/ApiDataIngestion.scala
### file%3A%2F%2F%2FC%3A%2FProyectos%2Fspark-datos-energia%2Fsrc%2Fmain%2Fscala%2Fexample%2FApiDataIngestion.scala:4: error: illegal start of definition `identifier`
º
^

occurred in the presentation compiler.

presentation compiler configuration:
Scala version: 2.12.19
Classpath:
<HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\2.12.19\scala-library-2.12.19.jar [exists ]
Options:



action parameters:
uri: file:///C:/Proyectos/spark-datos-energia/src/main/scala/example/ApiDataIngestion.scala
text:
```scala
package example


º
import io.circe.Json
import io.circe.parser._
import io.circe.syntax._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.{File, PrintWriter}

import sttp.client4._

object ApiDataDownloader {

    def main(args: Array[String]): Unit = {
    
        val sort: Option[String] = None
        val query = "http language: scala"

        val request = basicRequest.get(uri"https://api.github.com/search/repositories?q=$query&sort=$sort")

        val backend = DefaultSyncBackend()
        val response = request.send(backend)

        println(response.header("Content-Length"))

        println(response.body)

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
    
    s"$baseUrl?$queryString"
    }


    //Llamada a la api

    def getApiData(apiUrl: String): Either[String, String] = {
        
        
    }

  
}
```



#### Error stacktrace:

```
scala.meta.internal.parsers.Reporter.syntaxError(Reporter.scala:16)
	scala.meta.internal.parsers.Reporter.syntaxError$(Reporter.scala:16)
	scala.meta.internal.parsers.Reporter$$anon$1.syntaxError(Reporter.scala:22)
	scala.meta.internal.parsers.Reporter.syntaxError(Reporter.scala:17)
	scala.meta.internal.parsers.Reporter.syntaxError$(Reporter.scala:17)
	scala.meta.internal.parsers.Reporter$$anon$1.syntaxError(Reporter.scala:22)
	scala.meta.internal.parsers.ScalametaParser.statSeqBuf(ScalametaParser.scala:4109)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$statSeq$1(ScalametaParser.scala:4096)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$statSeq$1$adapted(ScalametaParser.scala:4096)
	scala.meta.internal.parsers.ScalametaParser.scala$meta$internal$parsers$ScalametaParser$$listBy(ScalametaParser.scala:562)
	scala.meta.internal.parsers.ScalametaParser.statSeq(ScalametaParser.scala:4096)
	scala.meta.internal.parsers.ScalametaParser.bracelessPackageStats$1(ScalametaParser.scala:4285)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$source$1(ScalametaParser.scala:4288)
	scala.meta.internal.parsers.ScalametaParser.atPos(ScalametaParser.scala:325)
	scala.meta.internal.parsers.ScalametaParser.autoPos(ScalametaParser.scala:369)
	scala.meta.internal.parsers.ScalametaParser.source(ScalametaParser.scala:4264)
	scala.meta.internal.parsers.ScalametaParser.entrypointSource(ScalametaParser.scala:4291)
	scala.meta.internal.parsers.ScalametaParser.parseSourceImpl(ScalametaParser.scala:119)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$parseSource$1(ScalametaParser.scala:116)
	scala.meta.internal.parsers.ScalametaParser.parseRuleAfterBOF(ScalametaParser.scala:58)
	scala.meta.internal.parsers.ScalametaParser.parseRule(ScalametaParser.scala:53)
	scala.meta.internal.parsers.ScalametaParser.parseSource(ScalametaParser.scala:116)
	scala.meta.parsers.Parse$.$anonfun$parseSource$1(Parse.scala:30)
	scala.meta.parsers.Parse$$anon$1.apply(Parse.scala:37)
	scala.meta.parsers.Api$XtensionParseDialectInput.parse(Api.scala:22)
	scala.meta.internal.semanticdb.scalac.ParseOps$XtensionCompilationUnitSource.toSource(ParseOps.scala:15)
	scala.meta.internal.semanticdb.scalac.TextDocumentOps$XtensionCompilationUnitDocument.toTextDocument(TextDocumentOps.scala:161)
	scala.meta.internal.pc.SemanticdbTextDocumentProvider.textDocument(SemanticdbTextDocumentProvider.scala:54)
	scala.meta.internal.pc.ScalaPresentationCompiler.$anonfun$semanticdbTextDocument$1(ScalaPresentationCompiler.scala:469)
```
#### Short summary: 

file%3A%2F%2F%2FC%3A%2FProyectos%2Fspark-datos-energia%2Fsrc%2Fmain%2Fscala%2Fexample%2FApiDataIngestion.scala:4: error: illegal start of definition `identifier`
º
^