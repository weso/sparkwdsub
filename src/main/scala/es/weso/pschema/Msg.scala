package es.weso.pschema

import es.weso.collection.Bag
import cats.implicits._

case class Msg[L,P: Ordering](
    validate: Set[L] = Set(), 
    outgoing: Option[Bag[P]] = None
    ) extends Serializable {

    def merge(other: Msg[L,P]): Msg[L,P] = {
      Msg(
        validate = this.validate.union(other.validate),
        outgoing = (this.outgoing, other.outgoing) match {
          case (None, None) => None
          case (Some(b), None) => Some(b)
          case (None, Some(b)) => Some(b)
          case (Some(b1), Some(b2)) => Some(b1.union(b2))
        }
      )
    }

    override def toString = s"Msg = ${
      if (validate.isEmpty) "" else "Validate: " + validate.map(_.toString).mkString(",")
    }${
      outgoing match {
        case None => ""
        case Some(bag) => bag.toSeq.map(_.toString).mkString(",")
      }
    }"
  } 

object Msg {
    def validate[L,P: Ordering](shapes: Set[L]): Msg[L,P] = Msg[L,P](validate = shapes, outgoing = none[Bag[P]])
    def outgoing[L,P: Ordering](arcs: Bag[P]): Msg[L,P] = Msg[L,P](validate = Set[L](), outgoing = Some(arcs))
}
