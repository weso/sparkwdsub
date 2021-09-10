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

class PSchemaSuite extends FunSuite 
  with SparkSessionTestWrapper with DatasetComparer with RDDComparer {

  protected def sort(
    ps: List[(String, List[String], List[String])]
    ): List[(String, List[String], List[String])] = 
    ps.map{ 
      case (p, vs, es) => (p, vs.sorted, es.sorted) 
    }.sortWith(_._1 < _._1) 

  import spark.implicits._

  def testCase(
   name: String,
   gb: GraphBuilder[Entity,Statement], 
   schema: Schema,
   initialLabel: ShapeLabel,
   expected: List[(String, List[String], List[String])],
   verbose: Boolean,
   maxIterations: Int = Int.MaxValue,
 )(implicit loc: munit.Location): Unit = {
 test(name) { 
  val graph = buildGraph(gb, spark.sparkContext)
  val validatedGraph = 
   PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](
     graph, initialLabel, maxIterations, verbose)(
     schema.checkLocal _,
     schema.checkNeighs _ ,
     schema.getTripleConstraints _ ,
     _.id
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
 }

}