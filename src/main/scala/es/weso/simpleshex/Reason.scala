package es.weso.simpleshex

import es.weso.collection.Bag
import es.weso.rbe.Rbe
import cats._
import cats.data._
import es.weso.rbe.RbeError
import es.weso.rbe.interval.IntOrUnbounded

sealed abstract class Reason extends Product with Serializable
case class NoValueForProperty(prop: Property) extends Reason
case class ValueIsNot(expectedId: String) extends Reason
case class ShapeNotFound(shapeLabel: ShapeLabel, schema: Schema) extends Reason
case class NoMatch(bag: Bag[(PropertyId,ShapeLabel)], rbe: Rbe[(PropertyId, ShapeLabel)], errors: NonEmptyList[RbeError]) extends Reason
case class NoValueValueSet(value: Value, valueSet: Set[Value]) extends Reason
case class NoStringDatatype(value: Value) extends Reason
case class ErrorsMatching(es: List[Reason]) extends Reason
case class CardinalityError(count: Int, min: Int, max: IntOrUnbounded) extends Reason
