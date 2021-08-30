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
import es.weso.rdf.nodes.{ 
  Lang => _,
  _
}

class ShapeExprSuite extends FunSuite {
  lazy val wdeStr = "http://www.wikidata.org/entity/"
  lazy val wde = IRI("http://www.wikidata.org/entity/")

  test("checkLocal") {
    val q5Id = ItemId("Q5", wde + "Q5")
    val q5 = Item(q5Id, 5L, Map(Lang("en") -> "Human"), Map(), Map(), wdeStr, List(), List())
    val q6Id = ItemId("Q6", wde + "Q6")
    val q6 = Item(q6Id, 6L, Map(Lang("en") -> "Researcher"), Map(), Map(), wdeStr, List(), List())
    val p31 = PropertyRecord(PropertyId.fromIRI(wde + "P31"),31L)
    // val s1: Statement = LocalStatement(p31,q5Id,List())
    val q80Id = ItemId("Q80", wde + "Q80")
    /* val q80 = Item(q80Id, 80L, "Tim", wdeStr, 
      List(p31)
    ) */


    assertEquals(1,1)
  }
}