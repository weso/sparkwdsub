package es.weso.pschema

import es.weso.simpleshex._
import es.weso.wbmodel._
import es.weso.wbmodel.Value._
import es.weso.rbe.interval._
import es.weso.graphxhelpers.GraphBuilder._
import es.weso.rdf.nodes._

class SampleSchemas extends PSchemaSuite {

  val schemaResearcher = Schema(
    Map(
      Start -> ShapeRef(IRILabel(IRI("Researcher"))),
      IRILabel(IRI("Researcher")) -> Shape(None,false,List(),Some(EachOf(List(
        TripleConstraintRef(Pid(31), ShapeRef(IRILabel(IRI("Human"))),1,IntLimit(1)),
        TripleConstraintRef(Pid(19), ShapeRef(IRILabel(IRI("Place"))),1,IntLimit(1))
      )))),
      IRILabel(IRI("Place")) -> Shape(None, false, List(), Some(EachOf(List(
        TripleConstraintRef(Pid(17), ShapeRef(IRILabel(IRI("Country"))),1,IntLimit(1))
      )))),
      IRILabel(IRI("Country")) -> EmptyExpr,
      IRILabel(IRI("Human")) -> 
       ValueSet(None,List(
         EntityIdValueSetValue(EntityId.fromIri(IRI("http://www.wikidata.org/entity/Q5")))
       ))
    )
  )

  val schemaSimple = Schema(
     Map(
       IRILabel(IRI("Human")) -> ValueSet(None,List(EntityIdValueSetValue(EntityId.fromIri(IRI("http://www.wikidata.org/entity/Q5"))))) 
     ), 
     start = Some(
       Shape(None, false,List(), Some(TripleConstraintRef(Pid(31), ShapeRef(IRILabel(IRI("Human"))),1,IntLimit(1))))
     )
    )

    val simpleGraph1: GraphBuilder[Entity, Statement] = for {
      human <- Q(5, "Human")
      timBl <- Q(80, "Tim Beners-Lee")
      instanceOf <- P(31, "instanceOf")
    } yield {
      vertexEdges(List(
        triple(timBl, instanceOf.prec, human)
      ))
    }
  

  val simpleGraph2: GraphBuilder[Entity, Statement] = for {
      human <- Q(5, "Human")
      dAdams <- Q(42, "Douglas Adams")
      timBl <- Q(80, "Tim Beners-Lee")
      instanceOf <- P(31, "instanceOf")
      cern <- Q(42944, "CERN")
      uk <- Q(145, "UK")
      paAward <- Q(3320352, "Princess of Asturias Award") 
      spain <- Q(29, "Spain")
      country <- P(17,"country")
      employer <- P(108,"employer")
      birthPlace <- P(19, "place of birth")
      london <- Q(84, "London")
      awardReceived <- P(166, "award received")
      togetherWith <- P(1706, "together with")
      y1980 = Date("1980")
      y1984 = Date("1984")
      y1994 = Date("1994")
      y2002 = Date("2002")
      y2013 = Date("2013")
      vintCerf <- Q(92743, "Vinton Cerf")
      start <- P(580, "start time")
      end <- P(582, "end time")
      time <- P(585, "point in time")
    } yield {
      vertexEdges(List(
        triple(timBl, instanceOf.prec , human),
        triple(timBl, birthPlace.prec, london),
//        tripleq(timBl, employer.pid, cern, List(Qualifier(start.pid, y1980.pid), Qualifier(end, y1980))),
//        tripleq(timBl, employer, cern, List(Qualifier(start, y1984), Qualifier(end, y1994))),
//        tripleq(timBl, awardReceived, paAward, List(Qualifier(togetherWith, vintCerf), Qualifier(time, y2002))),
        triple(dAdams, instanceOf.prec, human), 
        triple(paAward, country.prec, spain),
        triple(vintCerf,instanceOf.prec, human),
     //   tripleq(vintCerf, awardReceived.pid, paAward, List(Qualifier(togetherWith, timBl), Qualifier(time, y2002))),
     //   tripleq(cern, awardReceived, paAward, List(Qualifier(time,y2013)))
      ))
    }


{
   val graph = simpleGraph1
   val schema = schemaSimple
   val expected: List[(String,List[String],List[String])] = List(
     ("Q5", List("Human"), List("Start")), 
     ("Q80", List("Start"), List())
    )
   testCase("Simple graph", graph, schema, Start, expected, false)
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
       IRILabel(IRI("Start")) -> 
        Shape(None,false,List(),Some(TripleConstraintRef(Pid(31), ShapeRef(IRILabel(IRI("Human"))),1,IntLimit(1)))),
       IRILabel(IRI("Human")) -> 
        ValueSet(None,List(
          EntityIdValueSetValue(EntityId.fromIri(IRI("http://www.wikidata.org/entity/Q5")))
          )) 
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
     false,
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
   testCase("Simple recursion", gb, schema, IRILabel(IRI("Person")), expected, false)
  }


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