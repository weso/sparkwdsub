package es.weso.wdsub.spark.simpleshex

import es.weso.collection.Bag
import es.weso.rbe.Rbe
import cats.data._
import es.weso.rbe.RbeError
import es.weso.rbe.interval.IntOrUnbounded
import es.weso.wdsub.spark.wbmodel._

sealed abstract class Reason extends Product with Serializable
case class NoValueForProperty(prop: Property) extends Reason
case class ValueIsNot(expectedId: String) extends Reason
case class ShapeNotFound(shapeLabel: ShapeLabel, schema: Schema) extends Reason
case class NoMatch(bag: Bag[(PropertyId,ShapeLabel)], rbe: Rbe[(PropertyId, ShapeLabel)], errors: NonEmptyList[RbeError]) extends Reason
case class NoValueValueSet(value: Value, valueSet: List[ValueSetValue]) extends Reason
case class NoStringDatatype(value: Value) extends Reason
case class NoDateDatatype(value: Value) extends Reason
case class ErrorsMatching(es: List[Reason]) extends Reason
case class CardinalityError(p: PropertyId, count: Int, min: Int, max: IntOrUnbounded) extends Reason
case class WaitingForFailed(es: Set[(Value, PropertyId, ShapeLabel)]) extends Reason
case class MatchNot(bag: Bag[(PropertyId,ShapeLabel)], rbe: Rbe[(PropertyId, ShapeLabel)]) extends Reason
case class ShapeOr_AllFailed(es: List[Reason]) extends Reason
case class NotImplemented(msg: String) extends Reason
case class NotAllowedNotInExtra(notAllowed: List[(PropertyId, Int)]) extends Reason
case class FailedPropsNotExtra(ps: Set[(PropertyId, ShapeLabel)]) extends Reason
case class NullEntity(fromLabel: ShapeLabel) extends Reason
case class NoneMatchShapeOr(entity: Entity, so: ShapeOr) extends Reason
