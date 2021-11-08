package es.weso.wdsub.spark.pschema 

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests._
import munit._
import es.weso.wdsub.spark.simpleshex._
import es.weso.wdsub.spark.wbmodel.Value._
import es.weso.wdsub.spark.wbmodel._
import es.weso.wdsub.spark.graphxhelpers.GraphBuilder._
import org.apache.spark.graphx.VertexRDD
import es.weso.rbe.interval._
import scala.collection.immutable.SortedSet
import es.weso.rdf.nodes._
import org.apache.spark.graphx._


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
  val validatedGraph = mkValidatedGraph(graph,schema,initialLabel, maxIterations, verbose)
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

 def mkValidatedGraph(
  graph: Graph[Entity,Statement], 
  schema: Schema, 
  initialLabel: ShapeLabel,
  maxIterations: Int,
  verbose: Boolean
  ): Graph[Shaped[Entity,ShapeLabel,Reason,PropertyId], Statement] = 
   PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](
           graph, initialLabel, maxIterations, verbose)(
           schema.checkLocal _,
           schema.checkNeighs _ ,
           schema.getTripleConstraints _ ,
           _.id
    )


 def testCaseStr(
   name: String,
   gb: GraphBuilder[Entity,Statement], 
   schemaStr: String,
   format: WShExFormat,
   expected: List[(String, List[String], List[String])],
   verbose: Boolean,
   maxIterations: Int = Int.MaxValue,
 )(implicit loc: munit.Location): Unit = {
 test(name) { 
  val graph = buildGraph(gb, spark.sparkContext)
  Schema.unsafeFromString(schemaStr, format) match {
    case Left(err) => fail(s"Error parsing schema: $err")
    case Right(schema) => 
      {
        val validatedGraph = mkValidatedGraph(graph,schema,Start,maxIterations,verbose)

        val vertices: List[(Long,Shaped[Entity,ShapeLabel,Reason,PropertyId])] = 
          validatedGraph.vertices.collect().toList

        val result: List[(String, List[String], List[String])] = sort(
         vertices
         .map{ case (_, sv) => ( sv.value, 
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
 }

}