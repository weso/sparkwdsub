package es.weso.wdsub.spark.pschema 

import munit._
import es.weso.wdsub.spark.simpleshex._
import es.weso.wdsub.spark.simpleshex.ShapeExpr._
import es.weso.wdsub.spark.wbmodel.Value._
import es.weso.wdsub.spark.wbmodel._
import es.weso.wdsub.spark.graphxhelpers.GraphBuilder._
import org.apache.spark.graphx.VertexRDD
import es.weso.rbe.interval._
import scala.collection.immutable.SortedSet
import es.weso.rdf.nodes._

class VProgSuite extends FunSuite {

  val schema1 = Schema(
     Map(
       label("S") -> ShapeAnd(None,List(
             shapeRef("SAux"), 
             shape(List(TripleConstraintRef(Pid(1), shapeRef("T"), 1, IntLimit(1))))
       )),
       label("SAux") -> ShapeOr(None, 
             List(shape(
               List(TripleConstraintRef(Pid(31), shapeRef("H"),1,IntLimit(1)))
             ),
             shape(
               List(TripleConstraintRef(Pid(279), shapeRef("SAux"), 1, IntLimit(1)))
             )
           )),
       label("H") -> valueSet(List(qid(5))),
       label("T") -> valueSet(List(qid(4)))
  ))

  val paperSchema = Schema(
    Map(
      Start -> ShapeRef(IRILabel(IRI("Researcher"))),
      IRILabel(IRI("Researcher")) -> Shape(None,false,List(),Some(EachOf(List(
        TripleConstraintRef(Pid(31), ShapeRef(IRILabel(IRI("Human"))),1,IntLimit(1)),
        TripleConstraintLocal(Pid(569), DateDatatype, 0, IntLimit(1)),
        TripleConstraintRef(Pid(19), ShapeRef(IRILabel(IRI("Place"))),1,IntLimit(1))
      )))),
      IRILabel(IRI("Place")) -> Shape(None, false, List(), Some(EachOf(List(
        TripleConstraintRef(Pid(17), ShapeRef(IRILabel(IRI("Country"))),1,IntLimit(1))
      )))),
      IRILabel(IRI("Country")) -> EmptyExpr,
      IRILabel(IRI("Human")) -> ValueSet(None,List(EntityIdValueSetValue(EntityId.fromIri(IRI("http://www.wikidata.org/entity/Q5")))))
    )
  )

  def testCase(
     name: String,
     schema: Schema,
     v: Shaped[Entity, ShapeLabel, Reason, PropertyId],
     lbl: ShapeLabel,
     msgLabel: MsgLabel[ShapeLabel, Reason, PropertyId],
     expected: Shaped[Entity, ShapeLabel, Reason, PropertyId],
     verbose: Boolean = false
   )(implicit loc: munit.Location): Unit = {
   test(name) { 
     val pschema2 = 
             new PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](schema.checkLocal, schema.checkNeighs, schema.getTripleConstraints, _.id)
     val newValue = pschema2.vProgLabel(v,lbl,msgLabel)
     if (verbose) {
       println(s"""|Label   : $lbl
                   |MsgLabel: $msgLabel
                   |checkLocal($lbl,${v.value})=${schema.checkLocal(lbl,v.value)}
                   |Before  : $v
                   |After   : $newValue
                   |Expected: $expected 
                   |""".stripMargin)
     }
     assertEquals(newValue, expected)
   }
  }


  testCase(
    "Validate message", 
    schema1, 
    Shaped.empty[Entity,ShapeLabel,Reason,PropertyId](Qid(5,"Q5",5)),
    label("H"),
    ValidateLabel,
    Shaped.empty[Entity,ShapeLabel,Reason,PropertyId](Qid(5,"Q5",5)).addOkShape(label("H")),
    true
    ) 

 testCase(
    "Validate message", 
    paperSchema, 
    Shaped.empty[Entity,ShapeLabel,Reason,PropertyId](Qid(80,"Q80",80))
    .withWaitingFor(
     label("Researcher"), 
     Set(
       DependTriple(0,Pid(31),label("Human")),
       DependTriple(10,Pid(19),label("Place"))
     ),
     Set(), Set() 
    ),
    label("Researcher"),
    MsgLabel.checkedOk(DependTriple(0,Pid(31),label("Human"))),
    Shaped.empty[Entity,ShapeLabel,Reason,PropertyId](Qid(80,"Q80",80))
    .withWaitingFor(
         label("Researcher"), 
         Set(
           DependTriple(10,Pid(19),label("Place"))
         ),
         Set(DependTriple(0,Pid(31),label("Human"))), 
         Set() 
    ),
    true
    ) 

}