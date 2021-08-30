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
import es.weso.rdf.nodes.{Lang => _, _}
import DumpUtils._
import org.apache.spark.rdd.RDD

class CheckLocalSuite extends FunSuite {

 def testCase(
   name: String,
   label: ShapeLabel, 
   entity: Entity,
   schemaStr: String,
   fromLabel: ShapeLabel,
   expected: Set[ShapeLabel],
  )(implicit loc: munit.Location): Unit = {
 test(name) { 
   Schema.unsafeFromString(schemaStr, CompactFormat).fold(
     err => fail(s"Error parsing schema: $err\nSchema: $schemaStr"),
     schema => {
       schema.get(label) match {
         case None => fail(s"Not found label $label")
         case Some(se) => se.checkLocal(entity,IRILabel(IRI("fromLabel")),schema).fold(
          err => fail(s"""|Error checking local
                          |Entity: $se,$entity
                          |fromLabel: $fromLabel
                          |Err: $err
                          |""".stripMargin),
          s => assertEquals(s,expected)
         )
       }
    })
  }
 }

{
  val schemaStr = 
   """|prefix wde: <http://www.wikidata.org/entity/>
      |
      |Start = @<City>
      |<City> EXTRA wde:P31 {
      | wde:P31 @<CityCode> 
      |}
      |<CityCode> [ wde:Q515 ]
      |""".stripMargin

  val cityCode = IRILabel(IRI("CityCode"))
  val fromLabel = IRILabel(IRI("fromLabel"))
  val entity = 
    Item(
      ItemId("Q515", IRI("http://www.wikidata.org/entity/Q515")), 
      515L, 
      Map(Lang("en") -> "CityCode"), 
      Map(), 
      Map(), 
      "http://www.wikidata.org/entity/", 
      List(), 
      List())
  testCase("checkLocal valueSet", cityCode, entity, schemaStr, fromLabel, Set[ShapeLabel]())
}

}