package es.weso.pschema 

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests._
import munit._
import es.weso.simpleshex._
import es.weso.wbmodel.Value._
import es.weso.wbmodel._
import es.weso.graphxhelpers.GraphBuilder._
import org.apache.spark.graphx.VertexRDD
import es.weso.rbe.interval._
import scala.collection.immutable.SortedSet
import es.weso.rdf.nodes._
import DumpUtils._
import org.apache.spark.rdd.RDD


class DumpSuite extends FunSuite 
  with SparkSessionTestWrapper with DatasetComparer with RDDComparer {

  import spark.implicits._

  lazy val sc = spark.sparkContext
  lazy val lineParser = LineParser()

def testCase(
   name: String,
   dumpStr: String, 
   schemaStr: String,
   expected: List[(String, List[String], List[String])],
   maxIterations: Int = Int.MaxValue,
  )(implicit loc: munit.Location): Unit = {
 test(name) { 
  val graph = lineParser.dump2Graph(dumpStr,sc)
  val initialLabel = Start

  Schema.unsafeFromString(schemaStr, CompactFormat).fold(
    err => fail(s"Error parsing schema: $err"),
    schema => {
        val validatedGraph = 
   PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](
     graph, initialLabel, maxIterations)(
     schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
   )

  val vertices: List[(Long,Shaped[Entity,ShapeLabel,Reason,PropertyId])] = 
        validatedGraph.vertices.collect().toList

  val result: List[(String, List[String], List[String])] = 
   sort(
     vertices
     .map{ case (_, sv) => 
      ( sv.value, 
        sv.okShapes.map(_.name).toList, 
        sv.noShapes.map(_.name).toList
      )}
      .collect { 
       case (e: Entity, okShapes, noShapes) => 
        (e.entityId.id, okShapes, noShapes) 
       }
      )
    assertEquals(result,expected)
    }  
   )
  }
 }

private def sort(
    ps: List[(String, List[String], List[String])]
    ): List[(String, List[String], List[String])] = 
    ps.map{ 
      case (p, vs, es) => (p, vs.sorted, es.sorted) 
 }.sortWith(_._1 < _._1) 

 {
  val dumpStr = 
   """|[
      |{"type":"item","aliases":{},"labels":{},"descriptions":{},"sitelinks":{},"id":"Q62","claims":{"P31":[{"rank":"normal","mainsnak":{"snaktype":"value","property":"P31","datavalue":{"type":"wikibase-entityid","value":{"entity-type":"item","numeric-id":515}},"datatype":"wikibase-item"},"id":"Q62$b21e6bb2-4bb5-7692-d436-d2d22d6bf063","type":"statement"}]}},
      |{"type":"item","aliases":{},"labels":{},"descriptions":{},"sitelinks":{},"id":"Q68","claims":{"P31":[{"rank":"normal","mainsnak":{"snaktype":"value","property":"P31","datavalue":{"type":"wikibase-entityid","value":{"entity-type":"item","numeric-id":12468333}},"datatype":"wikibase-item"},"id":"Q68$2C1D1AAF-A295-403E-AFCB-EB902DB5762F","type":"statement"}]}},
      |{"type":"item","aliases":{},"labels":{},"descriptions":{},"sitelinks":{},"id":"Q75","claims":{"P31":[{"rank":"normal","mainsnak":{"snaktype":"value","property":"P31","datavalue":{"type":"wikibase-entityid","value":{"entity-type":"item","numeric-id":1301371}},"datatype":"wikibase-item"},"id":"q75$09CE92CB-C019-4E99-B6A5-4460B5DC1AA2","type":"statement"}]}},
      |{"type":"item","aliases":{},"labels":{},"descriptions":{},"sitelinks":{},"id":"Q515","claims":{}},
      |{"type":"item","aliases":{},"labels":{},"descriptions":{},"sitelinks":{},"id":"Q12468333","claims":{}},
      |{"type":"item","aliases":{},"labels":{},"descriptions":{},"sitelinks":{},"id":"Q1301371","claims":{}}
      |]
      |""".stripMargin   
  val schemaStr = 
   """|prefix wde: <http://www.wikidata.org/entity/>
      |
      |Start = @<City>
      |<City> {
      | wde:P31 @<CityCode> 
      |}
      |<CityCode> [ wde:Q515 ]
      |""".stripMargin
  val expected = sort(List(
     ("Q12468333", Nil, List("City","CityCode")),
     ("Q1301371", Nil, List("City","CityCode")),
     ("Q62", List("City"), List()),
     ("Q68", List(), List("City")),
     ("Q75", List(), List("City")),
     ("Q515", List("CityCode"), List("City"))
     ))
  testCase("3lines",dumpStr,schemaStr,expected)
 }

}