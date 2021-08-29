package es.weso.sparkwdsub

import buildinfo._
import cats.effect._
import cats.implicits._
import com.monovore.decline._
import com.monovore.decline.effect._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import es.weso.rdf.nodes._
import es.weso.collection.Bag
import es.weso.graphxhelpers.GraphBuilder._
import es.weso.pschema._
import es.weso.wbmodel._
import es.weso.wbmodel.Value._
import org.apache.spark.sql.DataFrameStatFunctions
import es.weso.simpleshex._
//import es.weso.wikibase._
import org.wikidata.wdtk.datamodel.helpers.JsonDeserializer
import org.wikidata.wdtk.datamodel.helpers
import cats.effect.unsafe.implicits.global
import org.wikidata.wdtk.datamodel.interfaces.{
  StringValue => WDStringValue, 
  Value => WDValue, 
  Statement => WDStatement,
  _
}
import es.weso.rbe.interval.IntLimit
import es.weso.wbmodel.DumpUtils._
import java.nio.file.Path



object SparkWDSub extends CommandIOApp (
  name = "sparkwdsub",
  header = "Wikidata subsetting using SPARK and ShEx",
  version = BuildInfo.version
) {
  
  val defaultSite: String = "http://www.wikidata.org/entity/"
  val defaultLoggingLevel = "ERROR"
  val defaultMaxIterations = 20

  case class Dump(
   filePath: Path, 
   schemaPath: Path,
   outPath: Option[Path], 
   site: String,
   maxIterations: Int,
   verbose: Boolean,
   logLevel: String
  )
  val filePath = Opts.argument[Path](metavar="dumpFile")
  val schemaPath = Opts.option[Path]("schema", help="schema path", short="s", metavar="file")
  val outPath = Opts.option[Path]("out", help="output path", short="o", metavar="file").orNone
  val site = Opts.option[String]("site", help = s"Base url, default =$siteDefault").withDefault(defaultSite)
  val verbose = Opts.flag("verbose", "Verbose mode").orFalse
  val logLevel = Opts.option[String]("loggingLevel", s"Logging level (ERROR, WARN, INFO), default=${defaultLoggingLevel}").withDefault(defaultLoggingLevel)
  val maxIterations = Opts.option[Int]("maxIterations", s"Max iterations for Pregel algorithm, default =${defaultMaxIterations}").withDefault(defaultMaxIterations)

  val dump: Opts[Dump] = 
    Opts.subcommand("dump", "Process example dump file.") {
      (filePath, schemaPath, outPath, site, maxIterations, verbose, logLevel).mapN(Dump)
  }  

  override def main: Opts[IO[ExitCode]] = 
    dump.map {
      case Dump(filePath, schemaPath, outPath, site, maxIterations, verbose, logLevel) => 
        doDump(filePath, schemaPath, outPath, site, maxIterations, verbose, logLevel)
    } 
    
  def doDump(
    dumpFilePath: Path, 
    schemaPath: Path,
    outPath: Option[Path], 
    site: String, 
    maxIterations: Int,
    verbose: Boolean,
    logLevel: String): IO[ExitCode] = IO {

    val master = "local"
    val partitions = 1

    lazy val spark: SparkSession = SparkSession
      .builder()
      .master(master)
      .appName("spark wdsub")
      .config("spark.sql.shuffle.partitions", partitions)
      .getOrCreate()
  
    lazy val sc = spark.sparkContext
    sc.setLogLevel(logLevel) 

    lazy val lineParser = LineParser(site)
    lazy val dumpFileName = dumpFilePath.toFile().getAbsolutePath()

    val graph = lineParser.dumpPath2Graph(dumpFilePath,sc)
    val initialLabel = Start
    val schema = Schema.unsafeFromPath(schemaPath, CompactFormat)
    val validatedGraph: Graph[Shaped[Entity,ShapeLabel,Reason,PropertyId], Statement] = 
       PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](
         graph, initialLabel, maxIterations, verbose)(
           schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
         )

    if (verbose) {         
    println(s"""|-------------------------------
                |End of validation
                |-------------------------------""".stripMargin)   
    println(s"Results: ${validatedGraph.triplets.count()} triples")
    
    validatedGraph
    .vertices
    .filter(containsValidShapes)
    .map(getIdShapes(_))
    .collect()
    .foreach(println(_))
    }

    sc.stop()

    ExitCode.Success
  }


  def containsValidShapes(pair: (VertexId, Shaped[Entity,ShapeLabel,Reason, PropertyId])): Boolean = {
    val (_,v) = pair
    v.okShapes.nonEmpty
  }

  def getIdShapes(pair: (VertexId, Shaped[Entity,ShapeLabel,Reason, PropertyId])): (String, Set[String], Set[String]) = {
    val (_,v) = pair
    (v.value.entityId.id, v.okShapes.map(_.name), v.noShapes.map(_.name))
  }

}