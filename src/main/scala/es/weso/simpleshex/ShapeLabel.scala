package es.weso.simpleshex
import cats._
case class ShapeLabel(name: String) extends Serializable {
 override def toString: String = name
}

object ShapeLabel {
  implicit val showShapeLabel: Show[ShapeLabel] = Show.show(p => p.name)
  implicit val orderingByName: Ordering[ShapeLabel] = Ordering.by(_.name)
}
