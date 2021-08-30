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

  private def sort(
    ps: List[(String, List[String], List[String])]
    ): List[(String, List[String], List[String])] = 
    ps.map{ 
      case (p, vs, es) => (p, vs.sorted, es.sorted) 
    }.sortWith(_._1 < _._1) 

  import spark.implicits._

  // This test comes from spark-fast-tests README
  test("aliases a DataFrame") {
    val sourceDF = Seq(("jose"),("li"),("luisa")).toDF("name")
    val actualDF = sourceDF.select(col("name").alias("student"))
    val expectedDF = Seq(("jose"),("li"),("luisa")).toDF("student")
    assertSmallDatasetEquality(actualDF, expectedDF)
  }

  test("simple test about RDD") {
    val sourceRDD = spark.sparkContext.parallelize(Seq(("jose"),("li"))).map(_.toUpperCase())
    val expectedRDD = spark.sparkContext.parallelize(Seq(("JOSE"),("LI")))
    assertSmallRDDEquality(sourceRDD, expectedRDD)
  }

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
 }

 /*
{
   val graph = SampleSchemas.simpleGraph1
   val schema = SampleSchemas.schemaSimple
   val expected: List[(String,List[String],List[String])] = List(
     ("Q5", List("Human"), List("Start")), 
     ("Q80", List("Start"), List())
    )
   testCase("Simple graph", graph, schema, Start, expected)
} 

{
  val gb = for {
       instanceOf <- P(31, "instance of")
       timbl <- Q(80, "alice")
       antarctica <- Q(51, "antarctica")
       human <- Q(5, "Human")
       continent <- Q(5107, "Continent")
     } yield {
       vertexEdges(List(
         triple(antarctica, instanceOf.prec, continent),
         triple(timbl, instanceOf.prec, human)
       ))
     }
    val schema = Schema(
     Map(
       IRILabel(IRI("Start")) -> Shape(None,false,List(),Some(TripleConstraintRef(Pid(31), ShapeRef(IRILabel(IRI("Human"))),1,IntLimit(1)))),
       IRILabel(IRI("Human")) -> ValueSet(None,List(EntityIdValue(EntityId.fromIri(IRI("http://www.wikidata.org/entity/Q5"))))) 
     ))
    val expected = sort(List(
        ("Q5", List("Human"), List("Start")),
        ("Q51", List(), List("Start")), 
        ("Q5107", List(), List("Human", "Start")),
        ("Q80", List("Start"), List()),
    ))
   testCase(
     "Simple schema", 
     gb,schema,
     IRILabel(IRI("Start")),
     expected,
     5)
  }

 {
   val gb: GraphBuilder[Entity,Statement] = for {
      name <- P(1, "name")
      knows <- P(2, "knows")
      aliceBasic <- Q(1, "alice")
      alice = aliceBasic.withLocalStatement(name.prec,Str("Alice"))
    } yield {
      vertexEdges(List(
        triple(alice, knows.prec, alice),
      ))
    }  
   val schema = Schema(
      Map(
      IRILabel(IRI("Person")) -> Shape(None, false, List(), Some(EachOf(List(
        TripleConstraintLocal(Pid(1), StringDatatype, 1, IntLimit(1)),
        TripleConstraintRef(Pid(2), ShapeRef(IRILabel(IRI("Person"))),0,Unbounded)
      ))))
     ))
    val expected: List[(String,List[String], List[String])] = List(
         ("Q1", List("Person"),List()) 
        )
   testCase("Simple recursion", gb, schema, IRILabel(IRI("Person")), expected)
  }
*/
  {
   val gb: GraphBuilder[Entity,Statement] = for {
      name <- P(1, "name")
      aliceBasic <- Q(1, "alice")
      alice = aliceBasic.withLocalStatement(name.prec,Str("Alice"))
      bobBasic <- Q(2, "bob")
      bob = bobBasic.withLocalStatement(name.prec, Str("Robert"))
      carolBasic <- Q(3,"carol")
      carol = carolBasic.withLocalStatement(name.prec, Str("Carole"))
      knows <- P(2, "knows")
      dave <- Q(4, "dave")
    } yield {
      vertexEdges(List(
        triple(alice, knows.prec, bob),
        triple(alice, knows.prec, alice),
        triple(bob, knows.prec, carol),
        triple(dave, knows.prec, dave)
      ))
    }  
   val schema = Schema(
      Map(
      IRILabel(IRI("Person")) -> 
       Shape(None, false, List(), 
        Some(EachOf(List(
         TripleConstraintLocal(Pid(1), StringDatatype, 1, IntLimit(1)),
         TripleConstraintRef(Pid(2), ShapeRef(IRILabel(IRI("Person"))),0,Unbounded)
        )))
       )
     ))
      
   val expected: List[(String,List[String], List[String])] = List(
        ("Q1", List("Person"),List()), 
        ("Q2", List("Person"),List()),
        ("Q3", List("Person"), List()),
        ("Q4", List(), List("Person"))
        )
   
    testCase("Recursion person", gb,schema, IRILabel(IRI("Person")), expected, true)
  } 

}