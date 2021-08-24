package es.weso.simpleshex

import es.weso.collection.Bag
import es.weso.rbe.Rbe
import cats._
import cats.data._
import es.weso.rbe.RbeError

sealed abstract class Reason extends Product with Serializable
case class NoValueForProperty(prop: Property) extends Reason
case class ValueIsNot(expectedId: String) extends Reason
case class ShapeNotFound(shapeLabel: ShapeLabel, schema: Schema) extends Reason
case class NoMatch(bag: Bag[Value.PropertyId], rbe: Rbe[Value.PropertyId], errors: NonEmptyList[RbeError]) extends Reason
case class NoValueValueSet(value: Value, valueSet: Set[String]) extends Reason
case class NoStringDatatype(value: Value, fromLabel: ShapeLabel) extends Reason
