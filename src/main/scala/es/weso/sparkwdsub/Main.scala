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
import java.nio.file.Paths
import org.apache.hadoop.mapred.FileAlreadyExistsException

object Main {
  
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
   logLevel: String,
   keepShapes: Boolean
  )
  val filePath = Opts.argument[Path](metavar="dumpFile")
  val schemaPath = Opts.option[Path]("schema", help="schema path", short="s", metavar="file")
  val outPath = Opts.option[Path]("out", help="output path", short="o", metavar="file").orNone
  val site = Opts.option[String]("site", help = s"Base url, default =$siteDefault").withDefault(defaultSite)
  val verbose = Opts.flag("verbose", "Verbose mode").orFalse
  val logLevel = Opts.option[String]("loggingLevel", s"Logging level (ERROR, WARN, INFO), default=${defaultLoggingLevel}").withDefault(defaultLoggingLevel)
  val maxIterations = Opts.option[Int]("maxIterations", s"Max iterations for Pregel algorithm, default =${defaultMaxIterations}").withDefault(defaultMaxIterations)
  val keepShapes = Opts.flag("keepShapes", "Keep shapes in output").orTrue

  val dump: Opts[Dump] = 
    Opts.subcommand("dump", "Process example dump file.") {
      (filePath, schemaPath, outPath, site, maxIterations, verbose, logLevel, keepShapes).mapN(Dump)
  }  

  val dumpCommand = Command(
    name = "dump",
    header = "create dumps using ShEx based traversal",
  ) {
    dump
  }

  def main(args: Array[String]): Unit = {
    dumpCommand.parse(args).map {
      case Dump(filePath, schemaPath, outPath, site, maxIterations, verbose, logLevel, keepShapes) => 
        doDump(filePath, schemaPath, outPath, site, maxIterations, verbose, logLevel, keepShapes)
    }
/*    if (args.size != 3) {
      println(s"Usage: sparkwdsub dumpFilePath schemaPath outResultPath")
    }
    val filePath = Paths.get(args(0))
    val schemaPath = Paths.get(args(1))
    val outPath = Paths.get(args(2)) 
        
    doDump(filePath,schemaPath,Some(outPath),defaultSite,defaultMaxIterations,true,"ERROR")
    */
  }
    
  def doDump(
    dumpFilePath: Path, 
    schemaPath: Path,
    maybeOutPath: Option[Path], 
    site: String, 
    maxIterations: Int,
    verbose: Boolean,
    logLevel: String,
    keepShapes: Boolean
    ): Unit = {

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

    if (verbose) println(s"Schema parsed: $schema")

    val validatedGraph: Graph[Shaped[Entity,ShapeLabel,Reason,PropertyId], Statement] = 
       PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](
         graph, initialLabel, maxIterations, verbose)(
           schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
         )

    // Keep only that have OK shapes 
    val subGraph = validatedGraph.subgraph(filterEdges,filterVertices)

    val result = 
        graph2rdd(
          subGraph.mapVertices{ case (_,v) => 
            if (keepShapes) (v.value.withOkShapes(v.okShapes)) 
            else v.value
           }, keepShapes)
    

    // TODO. Serialize and write to output

    maybeOutPath match {
      case None => println(s"No output path")
      case Some(outPath) => {
        try {
         result.saveAsTextFile(
          outPath.toFile().getAbsolutePath())
        } catch {
          case _: FileAlreadyExistsException => println(s"Error writing to file: ${outPath}. File already exists")
          case exc: Throwable => println(s"Error writing File: ${exc.getMessage()}")
        }
      }
    }
    
    println(s"""|Result: ${result.count()} lines. 
                |Output path: ${maybeOutPath.getOrElse("<nothing>")}""".stripMargin)

    sc.stop()
  }

  def graph2rdd(g: Graph[Entity,Statement], showShapes: Boolean): RDD[String] = 
    g.vertices.map(_._2).map(ValueWriter.entity2JsonStr(_, showShapes))

  def filterEdges(t: EdgeTriplet[Shaped[Entity,ShapeLabel,Reason,PropertyId], Statement]): Boolean = {
    t.srcAttr.okShapes.nonEmpty
  }

  def filterVertices(id: VertexId, v: Shaped[Entity,ShapeLabel,Reason,PropertyId]): Boolean = {
    v.okShapes.nonEmpty
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