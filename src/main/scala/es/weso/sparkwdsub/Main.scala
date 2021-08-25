package es.weso.sparkwdsub

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import es.weso.rdf.nodes._
import scala.jdk.CollectionConverters._
import es.weso.collection.Bag
import es.weso.graphxhelpers.GraphBuilder._
import es.weso.pschema._
import org.apache.spark.sql.DataFrameStatFunctions
import es.weso.simpleshex._
//import es.weso.wikibase._
import org.wikidata.wdtk.datamodel.helpers.JsonDeserializer
import org.wikidata.wdtk.datamodel.helpers
import cats.effect.unsafe.implicits.global
import org.wikidata.wdtk.datamodel.interfaces.{
  StringValue => WDStringValue, 
  Value => WDValue, 
  _
}

object SimpleApp {

  lazy val PropertyIdDisplacement: Long = 10000000000L // We will assign vertex id's for items starting from 0 and for properties starting with this value
  lazy val StringIdDisplacement: Long = 20000000000L // We will assign vertex id's for items starting from 0 and for properties starting with this value

  def mkVertexId(value: WDValue): Long = {

    val item = """Q(\d+)""".r
    val prop = """P(\d+)""".r
    
    value match {
      case null => 0L
      case id: ItemIdValue => id.getId() match {
        case item(id) => id.toLong
      }
      case pd: PropertyIdValue => pd.getId() match {
        case prop(id) => id.toLong + PropertyIdDisplacement
      }
//      case sv: StringValue => sv.getString().hashCode() + StringIdDisplacement
      case _ => 0L
    }
  }

  def brackets(line: String): Boolean = line.replaceAll("\\s", "") match {
    case "[" => true
    case "]" => true
    case _ => false
  }

  def mkEntity(ed: EntityDocument): (Long, Value) = {
    val vertexId = mkVertexId(ed.getEntityId()) 
    ed match {
     case id: ItemDocument => { 
      val label = Option(id.findLabel("en")).getOrElse("")
      (vertexId, 
       Entity(id.getEntityId().getId(), vertexId, label, id.getEntityId().getSiteIri())
      ) 
      }
     case pd: PropertyDocument => {
      val label = Option(pd.findLabel("en")).getOrElse("")
      (vertexId, 
       Property(pd.getEntityId().getId(), vertexId, label,List(), pd.getEntityId().getSiteIri())
      )
     }
    }
  }

  type PropertyId = Long

  def mkProperties(ed: EntityDocument): Iterator[Edge[PropertyId]] = {
    ed match {
     case sd: StatementDocument => { 
       sd.getAllStatements().asScala.map(s => {
         val subjectId = mkVertexId(s.getSubject())     
         val propertyId = mkVertexId(s.getMainSnak().getPropertyId())
         val valueId = mkVertexId(s.getValue())
         Edge(subjectId, propertyId, valueId)
       }) 
     }
     case _ => Iterator[Edge[PropertyId]]()
    }
  }   


  def main(args: Array[String]) {

    val master = "local"
    val partitions = 1

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
    lazy val site: String = "http://www.wikidata.org/entity/"
    lazy val jsonDeserializer = new helpers.JsonDeserializer(site)

    val vertices: RDD[(Long,Value)] = 
      sc.textFile("examples/dump.json")
      .filter(!brackets(_))
      .map(line => { 
        val jsonDeserializer = new JsonDeserializer( "http://www.wikidata.org/entity/" )
        val entityDocument = jsonDeserializer.deserializeEntityDocument(line)
        mkEntity(entityDocument)
      })

/*    val edges: RDD[(VertexId,Edge[Property])] = 
      sc.textFile("examples/dump.json")
      .filter(!brackets(_))
      .map(line => { 
        val jsonDeserializer = new JsonDeserializer( "http://www.wikidata.org/entity/" )
        val entityDocument = jsonDeserializer.deserializeEntityDocument(line)
        mkProperties(entityDocument)
      })      
*/

/*    val edges: RDD[(VertexId,Entity)] = sc.textFile("examples/example1.edges").map(line => {
      val row = line.split(",")
      val src = row(0).toLong
      val dst = row(1).toLong
//      val prop = Property(row(2),row(3),row)
      Edge(src, dst, prop)
    })*/

    println(vertices.collect().map(_.toString()).mkString("\n"))
//    val schema: Schema = SampleSchemas.schemaSimple

//    println(s"Graph triplets: ${graph.triplets.count()}")
//    graph.triplets.collect().foreach(println(_))

//    val initialLabel = ShapeLabel("Start")

//    def cnvProperty(p: Property): PropertyId = p.id

/*    val validatedGraph: Graph[ShapedValue[Value,ShapeLabel,Reason,PropertyId], Property] = 
      PSchema[Value,Property,ShapeLabel,Reason, PropertyId](
        graph, initialLabel, 5)(
          schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
        )

    println(s"Validated graph: ${validatedGraph.triplets.count()} triples")
    validatedGraph.vertices.collect().foreach(println(_))
*/    
    sc.stop()
 
  }


}