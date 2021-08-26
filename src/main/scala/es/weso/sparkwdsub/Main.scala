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
import es.weso.simpleshex.Value._
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

  def mkEntity(ed: EntityDocument): (Long, Entity) = {
    val vertexId = mkVertexId(ed.getEntityId()) 
    ed match {
     case id: ItemDocument => { 
      val label = Option(id.findLabel("en")).getOrElse("")
      (vertexId, 
       Item(ItemId(id.getEntityId().getId()), vertexId, label, id.getEntityId().getSiteIri(), List())
      ) 
      }
     case pd: PropertyDocument => {
      val label = Option(pd.findLabel("en")).getOrElse("")
      (vertexId, 
       Property(PropertyId(pd.getEntityId().getId()), vertexId, label, pd.getEntityId().getSiteIri(), List())
      )
     }
    }
  }

  def mkStatement(s: WDStatement): Option[Edge[Statement]] = 
   s.getValue() match {
    case null => None
    case ev: EntityIdValue => {
      val subjectId = mkVertexId(s.getSubject())     
      val wdpid = s.getMainSnak().getPropertyId()
      val pid = PropertyId(wdpid.getId())
      val pVertex = mkVertexId(wdpid)
      val valueId = mkVertexId(ev)
      // TODO. Collect qualifiers
      Some(Edge(subjectId, valueId, Statement(PropertyRecord(pid,pVertex))))
    }
    case _ => None
   }

  def mkStatements(ed: EntityDocument): List[Edge[Statement]] = {
    ed match {
     case sd: StatementDocument => { 
       sd.getAllStatements().asScala.toList.map(mkStatement).flatten
     }
     case _ => List[Edge[Statement]]()
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

    val vertices: RDD[(Long,Entity)] = 
      sc.textFile("examples/dump.json")
      .filter(!brackets(_))
      .map(line => { 
        val jsonDeserializer = new JsonDeserializer( "http://www.wikidata.org/entity/" )
        val entityDocument = jsonDeserializer.deserializeEntityDocument(line)
        mkEntity(entityDocument)
      })

    val edges = 
      sc.textFile("examples/dump.json")
      .filter(!brackets(_))
      .map(line => { 
        val jsonDeserializer = new JsonDeserializer( "http://www.wikidata.org/entity/" )
        val entityDocument = jsonDeserializer.deserializeEntityDocument(line)
        mkStatements(entityDocument)
      }).flatMap(identity)


    println(vertices.collect().map(_.toString()).mkString("\n"))

    val graph = Graph(vertices,edges)
    val initialLabel = ShapeLabel("Start")
    val schema = Schema(Map(
      ShapeLabel("Start") -> TripleConstraintRef(Pid(31), ShapeRef(ShapeLabel("Human")),1,IntLimit(1)),
      ShapeLabel("Human") -> ValueSet(Set(ItemId("Q5"))) 
    ))

    val validatedGraph: Graph[Shaped[Entity,ShapeLabel,Reason,PropertyId], Statement] = 
      PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](
        graph, initialLabel, 5)(
          schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
        )

    println(s"Validated graph: ${validatedGraph.triplets.count()} triples")
    validatedGraph.vertices.collect().foreach(println(_))
    sc.stop()
 
  }


}