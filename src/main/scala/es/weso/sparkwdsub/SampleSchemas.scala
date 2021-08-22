package es.weso.sparkwdsub

import Helpers._
import es.weso.rbe.interval._

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

}