package es.weso.sparkwdsub

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

object SimpleApp {

  def main(args: Array[String]) {

    val master = "local"
    val partitions = 1
    val dumpFilePath = args(0)

    lazy val spark: SparkSession = SparkSession
      .builder()
      .master(master)
      .appName("spark wdsub")
      .config("spark.sql.shuffle.partitions", partitions)
      .getOrCreate()
  
    // val conf = new SparkConf().setAppName("Simple App").setMaster(master)
    // val sc = new SparkContext(conf)
    lazy val sc = spark.sparkContext
    sc.setLogLevel("ERROR") 
    // sc.setLogLevel("INFO") 
    lazy val site: String = "http://www.wikidata.org/entity/"
    lazy val lineParser = LineParser(site)

    val vertices: RDD[(Long,Entity)] = 
      sc.textFile(dumpFilePath)
      .filter(!brackets(_))
      .map(lineParser.line2Entity(_))

    val edges = 
      sc.textFile(dumpFilePath)
      .filter(!brackets(_))
      .map(lineParser.line2Statement(_))
      .flatMap(identity)

    println(s"Vertices: ")  
    println(vertices.collect().map(_.toString()).mkString("\n"))
    println(s"Edges: ")  
    println(edges.collect().map(_.toString()).mkString("\n"))
    println(s"---------------")

    val graph = Graph(vertices,edges)
    val initialLabel = Start
    val schemaStr = 
      """|prefix wde: <http://www.wikidata.org/entity/>
         |
         |Start = @<City>
         |<City> {
         | wde:P31 @<CityCode> 
         |}
         |<CityCode> [ wde:Q515 ]
         |""".stripMargin

    val eitherSchema = Schema.unsafeFromString(schemaStr, CompactFormat)

    eitherSchema.fold(
      e => println(s"Error parsing schema: $e"),
      schema => {
       val validatedGraph: Graph[Shaped[Entity,ShapeLabel,Reason,PropertyId], Statement] = 
       PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](
         graph, initialLabel, 5)(
           schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
         )

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
    )

    sc.stop()
 
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