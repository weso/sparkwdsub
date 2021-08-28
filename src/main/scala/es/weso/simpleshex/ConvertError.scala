package es.weso.simpleshex

import es.weso.rdf._
import es.weso.rdf.nodes._
import es.weso._
import cats.implicits._

sealed trait ConvertError extends Throwable
case class UnsupportedShapeExpr(se: shex.ShapeExpr) extends ConvertError
case class UnsupportedShape(s: shex.Shape) extends ConvertError
case class UnsupportedNodeConstraint(nc: shex.NodeConstraint) extends ConvertError
case class UnsupportedValueSetValue(v: shex.ValueSetValue) extends ConvertError
case class UnsupportedTripleConstraint(tc: shex.TripleConstraint) extends ConvertError
case class CastTripleConstraintError(te: TripleExpr) extends ConvertError
