package es.weso.pschema 

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests._
import munit._
import es.weso.simpleshex._
import es.weso.simpleshex.Value._
import es.weso.graphxhelpers.GraphBuilder._
import org.apache.spark.graphx.VertexRDD
import es.weso.rbe.interval._

class PSchemaSuite extends FunSuite 
  with SparkSessionTestWrapper with DatasetComparer with RDDComparer {

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

  test("Simple graph") {
     val graph = buildGraph(SampleSchemas.simpleGraph1, spark.sparkContext)
     val schema = SampleSchemas.schemaSimple
     val validatedGraph = PSchema[Value,Property,ShapeLabel,Reason, Value.PropertyId](
        graph, ShapeLabel("Start"), 5)(
          schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
        )
    val vertices: List[(Long,Shaped[Value,ShapeLabel,Reason,PropertyId])] = 
        validatedGraph.vertices.collect().toList
    val result: List[(String, Set[String])] = 
        vertices
        .map{ case (_, sv) => (sv.value, sv.shapesInfo.okShapes.map(_.name))}
        .collect { case (e: Entity, okShapes) => (e.id, okShapes)} 
    val expected: List[(String,Set[String])] = List(
        ("Q5", Set("Human")), 
        ("Q80", Set("Start"))
        )
    assertEquals(vertices.size,3)
    assertEquals(result,expected)
  }

  test("Recursion basic") {
   val gb = for {
      alice <- Q(1, "alice")
      bob <- Q(2, "bob")
      carol <- Q(3,"carol")
      name <- P(1, "name")
      knows <- P(2, "knows")
      aliceName <- Str("Alice")
      bobName <- Str("Robert")
      carolName <- Str("Carole")
      dave <- Q(4, "dave")
    } yield {
      vertexEdges(List(
        triple(alice, knows, bob),
        triple(alice, name, aliceName),
        triple(alice, knows, alice),
        triple(bob, name, bobName),
        triple(bob, knows, carol),
        triple(carol, name, carolName),
        triple(dave, knows, dave)
      ))
    }  
   val graph = buildGraph(gb, spark.sparkContext)
   val schema = Schema(
      Map(
      ShapeLabel("Person") -> EachOf(List(
        TripleConstraint("P1", ShapeRef(ShapeLabel("Str")),1,IntLimit(1)),
        TripleConstraint("P2", ShapeRef(ShapeLabel("Person")),0,Unbounded)
      )),
      ShapeLabel("Str") -> StringDatatype
     ))
      
     val validatedGraph = PSchema[Value,Property,ShapeLabel,Reason, PropertyId](
        graph, ShapeLabel("Person"), 5)(
          schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
        )
    val vertices: List[(Long,Shaped[Value,ShapeLabel,Reason,PropertyId])] = 
        validatedGraph.vertices.collect().toList
    val result: List[(String, Set[String], Set[String])] = 
        vertices
        .map{ 
          case (_, sv) => 
            (sv.value, 
             sv.shapesInfo.okShapes.map(_.name),
             sv.shapesInfo.noShapes.map(_.name)             
            )}
        .collect { 
          case (e: Entity, okShapes, noShapes) => 
            (e.id, okShapes, noShapes)
          } 
    val expected: List[(String,Set[String], Set[String])] = List(
        ("Q1", Set("Person"),Set()), 
        ("Q2", Set("Person"),Set()),
        ("Q3", Set("Person"), Set()),
        ("Q4", Set(), Set("Person"))
        )
    assertEquals(vertices.size,9)
    assertEquals(result.sortWith(_._1 < _._1),expected)
  }

}