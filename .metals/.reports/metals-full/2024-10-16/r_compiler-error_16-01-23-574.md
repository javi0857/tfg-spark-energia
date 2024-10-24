file:///C:/Proyectos/spark-datos-energia/src/main/scala/example/ApiDataIngestion.scala
### file%3A%2F%2F%2FC%3A%2FProyectos%2Fspark-datos-energia%2Fsrc%2Fmain%2Fscala%2Fexample%2FApiDataIngestion.scala:89: error: illegal start of simple pattern
    }
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


import sttp.client3._
import sttp.client3.circe._
import io.circe.Json
import io.circe.parser._
import io.circe.syntax._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.{File, PrintWriter}

import sttp.client4._

object ApiDataDownloader {

    def main(args: Array[String]): Unit = {
    
        //Defino las variables necesarias para hacer la llamada a la api

        val category = "balance"
        val widget = "balance-electrico"
        val time_trunc = "day"
        val lang = "es"
        val geo_trunc = "electric_system"
        val geo_limit = "ccaa"
        val geo_ids = "13"

        //Creo un rango con las fechas en el formato necesario para la llamada

        val rango = (2011 to 2024).map( year => (year + "-01-01T00:00", year + "-12-31T23:59"))

        //Recorro el rango de fechas haciendo llamadas a la api con los widgets que he definido en las variables

        val urisRango = rango.map {
            case (inicio, fin) => createUri(category, widget, inicio, fin, time_trunc,lang)
        }   

        urisRango.map(println)

        val ejemplourl = "https://apidatos.ree.es/es/datos/balance/balance-electrico?start_date=2011-01-01T00:00&end_date=2011-12-31T23:59&time_trunc=day"
        
        val response = getApiData(ejemplourl)
        
        println(response)

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
        
        val sort: Option[String] = None
        val query = "http language: scala"

        val request = basicRequest.get(uri"https://api.github.com/search/repositories?q=$query&sort=$sort")

        val backend = DefaultSyncBackend()
        val 
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
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.badPattern3(ScalametaParser.scala:2641)
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.badPattern3$(ScalametaParser.scala:2621)
	scala.meta.internal.parsers.ScalametaParser$noSeq$.badPattern3(ScalametaParser.scala:2722)
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.$anonfun$pattern3$1(ScalametaParser.scala:2603)
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.$anonfun$simplePattern$1(ScalametaParser.scala:2708)
	scala.meta.internal.parsers.ScalametaParser.autoEndPos(ScalametaParser.scala:372)
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.simplePattern(ScalametaParser.scala:2650)
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.simplePattern$(ScalametaParser.scala:2644)
	scala.meta.internal.parsers.ScalametaParser$noSeq$.simplePattern(ScalametaParser.scala:2722)
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.pattern3(ScalametaParser.scala:2603)
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.pattern3$(ScalametaParser.scala:2601)
	scala.meta.internal.parsers.ScalametaParser$noSeq$.pattern3(ScalametaParser.scala:2722)
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.pattern2(ScalametaParser.scala:2582)
	scala.meta.internal.parsers.ScalametaParser$SeqContextSensitive.pattern2$(ScalametaParser.scala:2581)
	scala.meta.internal.parsers.ScalametaParser$noSeq$.pattern2(ScalametaParser.scala:2722)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$patDefOrDcl$2(ScalametaParser.scala:3351)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$commaSeparated$1(ScalametaParser.scala:639)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$commaSeparated$1$adapted(ScalametaParser.scala:639)
	scala.meta.internal.parsers.ScalametaParser.iter$1(ScalametaParser.scala:629)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$tokenSeparated$1(ScalametaParser.scala:634)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$tokenSeparated$1$adapted(ScalametaParser.scala:622)
	scala.meta.internal.parsers.ScalametaParser.scala$meta$internal$parsers$ScalametaParser$$listBy(ScalametaParser.scala:562)
	scala.meta.internal.parsers.ScalametaParser.tokenSeparated(ScalametaParser.scala:622)
	scala.meta.internal.parsers.ScalametaParser.commaSeparatedWithIndex(ScalametaParser.scala:643)
	scala.meta.internal.parsers.ScalametaParser.commaSeparated(ScalametaParser.scala:639)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$patDefOrDcl$1(ScalametaParser.scala:3351)
	scala.meta.internal.parsers.ScalametaParser.autoEndPos(ScalametaParser.scala:372)
	scala.meta.internal.parsers.ScalametaParser.autoEndPos(ScalametaParser.scala:377)
	scala.meta.internal.parsers.ScalametaParser.patDefOrDcl(ScalametaParser.scala:3346)
	scala.meta.internal.parsers.ScalametaParser.defOrDclOrSecondaryCtor(ScalametaParser.scala:3315)
	scala.meta.internal.parsers.ScalametaParser.localDef(ScalametaParser.scala:4188)
	scala.meta.internal.parsers.ScalametaParser.iter$7(ScalametaParser.scala:4225)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$blockStatSeq$1(ScalametaParser.scala:4247)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$blockStatSeq$1$adapted(ScalametaParser.scala:4197)
	scala.meta.internal.parsers.ScalametaParser.scala$meta$internal$parsers$ScalametaParser$$listBy(ScalametaParser.scala:562)
	scala.meta.internal.parsers.ScalametaParser.blockStatSeq(ScalametaParser.scala:4197)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$blockOnBrace$2(ScalametaParser.scala:2385)
	scala.meta.internal.parsers.ScalametaParser.inBracesOnOpen(ScalametaParser.scala:265)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$blockOnBrace$1(ScalametaParser.scala:2383)
	scala.meta.internal.parsers.ScalametaParser.atPos(ScalametaParser.scala:325)
	scala.meta.internal.parsers.ScalametaParser.autoPos(ScalametaParser.scala:369)
	scala.meta.internal.parsers.ScalametaParser.blockOnBrace(ScalametaParser.scala:2383)
	scala.meta.internal.parsers.ScalametaParser.blockOnBrace(ScalametaParser.scala:2385)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$blockExprOnBrace$1(ScalametaParser.scala:2388)
	scala.meta.internal.parsers.ScalametaParser.blockExprPartial(ScalametaParser.scala:2367)
	scala.meta.internal.parsers.ScalametaParser.blockExprOnBrace(ScalametaParser.scala:2387)
	scala.meta.internal.parsers.ScalametaParser.simpleExpr0(ScalametaParser.scala:2079)
	scala.meta.internal.parsers.ScalametaParser.simpleExpr(ScalametaParser.scala:2061)
	scala.meta.internal.parsers.ScalametaParser.prefixExpr(ScalametaParser.scala:2058)
	scala.meta.internal.parsers.ScalametaParser.postfixExpr(ScalametaParser.scala:1924)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$expr$2(ScalametaParser.scala:1552)
	scala.meta.internal.parsers.ScalametaParser.atPosOpt(ScalametaParser.scala:327)
	scala.meta.internal.parsers.ScalametaParser.autoPosOpt(ScalametaParser.scala:370)
	scala.meta.internal.parsers.ScalametaParser.expr(ScalametaParser.scala:1480)
	scala.meta.internal.parsers.ScalametaParser.expr(ScalametaParser.scala:1381)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$funDefRest$1(ScalametaParser.scala:3547)
	scala.meta.internal.parsers.ScalametaParser.autoEndPos(ScalametaParser.scala:372)
	scala.meta.internal.parsers.ScalametaParser.autoEndPos(ScalametaParser.scala:377)
	scala.meta.internal.parsers.ScalametaParser.funDefRest(ScalametaParser.scala:3511)
	scala.meta.internal.parsers.ScalametaParser.funDefOrDclOrExtensionOrSecondaryCtor(ScalametaParser.scala:3460)
	scala.meta.internal.parsers.ScalametaParser.defOrDclOrSecondaryCtor(ScalametaParser.scala:3320)
	scala.meta.internal.parsers.ScalametaParser.nonLocalDefOrDcl(ScalametaParser.scala:3299)
	scala.meta.internal.parsers.ScalametaParser$$anonfun$templateStat$1.applyOrElse(ScalametaParser.scala:4150)
	scala.meta.internal.parsers.ScalametaParser$$anonfun$templateStat$1.applyOrElse(ScalametaParser.scala:4147)
	scala.PartialFunction.$anonfun$runWith$1$adapted(PartialFunction.scala:145)
	scala.meta.internal.parsers.ScalametaParser.statSeqBuf(ScalametaParser.scala:4107)
	scala.meta.internal.parsers.ScalametaParser.getStats$2(ScalametaParser.scala:4137)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$scala$meta$internal$parsers$ScalametaParser$$templateStatSeq$3(ScalametaParser.scala:4138)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$scala$meta$internal$parsers$ScalametaParser$$templateStatSeq$3$adapted(ScalametaParser.scala:4136)
	scala.meta.internal.parsers.ScalametaParser.scala$meta$internal$parsers$ScalametaParser$$listBy(ScalametaParser.scala:562)
	scala.meta.internal.parsers.ScalametaParser.scala$meta$internal$parsers$ScalametaParser$$templateStatSeq(ScalametaParser.scala:4136)
	scala.meta.internal.parsers.ScalametaParser.scala$meta$internal$parsers$ScalametaParser$$templateStatSeq(ScalametaParser.scala:4128)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$templateBody$1(ScalametaParser.scala:4006)
	scala.meta.internal.parsers.ScalametaParser.inBracesOr(ScalametaParser.scala:260)
	scala.meta.internal.parsers.ScalametaParser.inBraces(ScalametaParser.scala:256)
	scala.meta.internal.parsers.ScalametaParser.templateBody(ScalametaParser.scala:4006)
	scala.meta.internal.parsers.ScalametaParser.templateBodyOpt(ScalametaParser.scala:4009)
	scala.meta.internal.parsers.ScalametaParser.templateAfterExtends(ScalametaParser.scala:3960)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$templateOpt$1(ScalametaParser.scala:4001)
	scala.meta.internal.parsers.ScalametaParser.atPos(ScalametaParser.scala:325)
	scala.meta.internal.parsers.ScalametaParser.autoPos(ScalametaParser.scala:369)
	scala.meta.internal.parsers.ScalametaParser.templateOpt(ScalametaParser.scala:3993)
	scala.meta.internal.parsers.ScalametaParser.$anonfun$objectDef$1(ScalametaParser.scala:3722)
	scala.meta.internal.parsers.ScalametaParser.autoEndPos(ScalametaParser.scala:372)
	scala.meta.internal.parsers.ScalametaParser.autoEndPos(ScalametaParser.scala:377)
	scala.meta.internal.parsers.ScalametaParser.objectDef(ScalametaParser.scala:3714)
	scala.meta.internal.parsers.ScalametaParser.tmplDef(ScalametaParser.scala:3601)
	scala.meta.internal.parsers.ScalametaParser.topLevelTmplDef(ScalametaParser.scala:3589)
	scala.meta.internal.parsers.ScalametaParser$$anonfun$2.applyOrElse(ScalametaParser.scala:4121)
	scala.meta.internal.parsers.ScalametaParser$$anonfun$2.applyOrElse(ScalametaParser.scala:4115)
	scala.PartialFunction.$anonfun$runWith$1$adapted(PartialFunction.scala:145)
	scala.meta.internal.parsers.ScalametaParser.statSeqBuf(ScalametaParser.scala:4107)
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

file%3A%2F%2F%2FC%3A%2FProyectos%2Fspark-datos-energia%2Fsrc%2Fmain%2Fscala%2Fexample%2FApiDataIngestion.scala:89: error: illegal start of simple pattern
    }
    ^