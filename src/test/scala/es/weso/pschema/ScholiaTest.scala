package es.weso.wdsub.spark.pschema

import es.weso.wdsub.spark.simpleshex._
import es.weso.wdsub.spark.simpleshex.ShapeExpr._
import es.weso.wdsub.spark.wbmodel._
import es.weso.wdsub.spark.wbmodel.Value._
import es.weso.rbe.interval._
import es.weso.wdsub.spark.graphxhelpers.GraphBuilder._
import es.weso.rdf.nodes._

class ScholiaTest extends PSchemaSuite {

  val graph: GraphBuilder[Entity, Statement] = for {
       q1 <- Q(1,"Q721")    // a publication 
       q42 <- Q(42,"Q42") // an author
       q3 <- Q(3,"Q3")    // another node... 
       q5 <- Q(5,"Q5") // human
       q174396 <- Q(174396,"Q174396") // Elf
//       q4 <- Q(4, "Q4") // mythical ethnic group
       q183 <- Q(183, "Q183") // Germany
       p31 <- P(31,"P31") // instance of
       p27 <- P(27, "P27") // country
       p50 <- P(50, "P50") // author
     } yield {
       vertexEdges(List(
         triple(q1, p50, q42),
         triple(q42, p31, q5),
         triple(q42, p27, q183),
         triple(q3, p31, q5),
//         triple(q174396, p31, q4)
       ))
   }
 
 {
   val schemaStr: String = 
    """|PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#>
       |PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX :    <http://www.wikidata.org/entity/>
       |
       |start=@<publication>
       |
       |<publication> EXTRA :P31 {
       | :P50 @<author> +;
       |}
       |
       |<author> EXTRA :P31 {
       | :P31 @<human> ;
       | :P27 @<country_value> 
       |}
       |
       |<human> [ :Q5 ] 
       |<country_value> [ :Q183 ]
       |""".stripMargin
   
   val expected: List[(String,List[String],List[String])] = List(
    ("Q1", List("Start"), List()),
   // ("Q174396", List(), List("Start")),
    ("Q183", List("country_value"), List("Start")),
    ("Q3", List(), List("Start")),
   // ("Q4", List(), List("Start")),
    ("Q42", List("author"), List("Start")),
    ("Q5", List("human"), List("Start"))
   )
   testCaseStr("Scholia test with Q5", graph, schemaStr, CompactFormat, expected, true)
  } 

  {
   val schemaStrElf: String = 
    """|PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#>
       |PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX :    <http://www.wikidata.org/entity/>
       |
       |start=@<publication>
       |
       |<publication> {
       | :P50 @<author> +;
       |}
       |
       |<author> EXTRA :P31 {
       | :P31 @<elf> ;
       | :P27 @<country_value> 
       |}
       |
       |<elf> [ :Q174396 ] 
       |<country_value> [ :Q183 ]
       |""".stripMargin
   val expected: List[(String,List[String],List[String])] = List(
    ("Q1", List(), List("Start")),
    ("Q183", List("country_value"), List("Start")),
    ("Q3", List(), List("Start")),   
    ("Q42", List(), List("Start", "author")),
    ("Q5", List(), List("Start", "elf"))
   )
   testCaseStr("Scholia test with Q174396", graph, schemaStrElf, CompactFormat, expected, true)
  } 
}