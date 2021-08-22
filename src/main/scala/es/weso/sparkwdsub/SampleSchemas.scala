package es.weso.sparkwdsub

import Helpers._
import es.weso.rbe.interval._
import es.weso.pschema.GraphBuilder._

object SampleSchemas {

  val schemaResearcher = Schema(
    Map(
      ShapeLabel("Start") -> ShapeRef(ShapeLabel("Researcher")),
      ShapeLabel("Researcher") -> EachOf(List(
        TripleConstraint("P31", ShapeRef(ShapeLabel("Human")),1,IntLimit(1)),
        TripleConstraint("P19", ShapeRef(ShapeLabel("Place")),1,IntLimit(1))
      )),
      ShapeLabel("Place") -> EachOf(List(
        TripleConstraint("P17", ShapeRef(ShapeLabel("Country")),1,IntLimit(1))
      )),
      ShapeLabel("Country") -> EmptyExpr,
      ShapeLabel("Human") -> ValueSet(Set("Q5")) 
    )
  )

  val schemaSimple = Schema(
    Map(
      ShapeLabel("Start") -> TripleConstraint("P31", ShapeRef(ShapeLabel("Human")),1,IntLimit(1)),
      ShapeLabel("Human") -> ValueSet(Set("Q5")) 
    ))

    val simpleGraph1: GraphBuilder[Value, Property] = for {
      human <- Q(5, "Human")
      timBl <- Q(80, "Tim Beners-Lee")
      instanceOf <- P(31, "instanceOf")
    } yield {
      vertexEdges(
        (timBl, instanceOf, human, List()),
      )
    }
  

  val simpleGraph2: GraphBuilder[Value, Property] = for {
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
      y1980 <- Date("1980")
      y1984 <- Date("1984")
      y1994 <- Date("1994")
      y2002 <- Date("2002")
      y2013 <- Date("2013")
      vintCerf <- Q(92743, "Vinton Cerf")
      start <- P(580, "start time")
      end <- P(582, "end time")
      time <- P(585, "point in time")
    } yield {
      vertexEdges(
        (timBl, instanceOf, human, List()),
        (timBl, birthPlace, london, List()),
        (timBl, employer, cern, List(Qualifier(start, y1980), Qualifier(end, y1980))),
        (timBl, employer, cern, List(Qualifier(start, y1984), Qualifier(end, y1994))),
        (timBl, awardReceived, paAward, List(Qualifier(togetherWith, vintCerf), Qualifier(time, y2002))),
        (dAdams, instanceOf, human, List()), 
        (paAward, country, spain, List()),
        (vintCerf,instanceOf, human, List()),
        (vintCerf, awardReceived, paAward, List(Qualifier(togetherWith, timBl), Qualifier(time, y2002))),
        (cern, awardReceived, paAward, List(Qualifier(time,y2013)))
      )
    }


}