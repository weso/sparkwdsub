package es.weso.pschema

import es.weso.collection.Bag
import cats.implicits._

case class Msg[VD, L,P: Ordering](
    validate: Set[L] = Set(), 
    outgoing: Option[Bag[(P,L)]] = None,
    waitFor: Map[VD, Set[(P,L)]] = Map()
    ) extends Serializable {

    def merge(other: Msg[VD, L, P]): Msg[VD, L, P] = {
      Msg(
        validate = this.validate.union(other.validate),
        outgoing = (this.outgoing, other.outgoing) match {
          case (None, None) => None
          case (Some(b), None) => Some(b)
          case (None, Some(b)) => Some(b)
          case (Some(b1), Some(b2)) => Some(b1.union(b2))
        },
        waitFor = this.waitFor ++ other.waitFor
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

  def validate[VD,L,P: Ordering](
    shapes: Set[L]
    ): Msg[VD,L,P] = 
      Msg[VD,L,P](validate = shapes, outgoing = none[Bag[(P,L)]], waitFor = Map[VD,Set[(P,L)]]())

  def outgoing[VD, L,P: Ordering](
    arcs: Bag[(P,L)]
    ): Msg[VD, L, P] = 
      Msg[VD, L, P](validate = Set[L](), outgoing = Some(arcs), waitFor = Map[VD,Set[(P,L)]]())

  def waitFor[VD,L,P:Ordering](e: VD, arc:(P,L)): Msg[VD,L,P] = 
      Msg[VD, L, P](validate = Set[L](), outgoing = None, waitFor = Map(e -> Set(arc)))

}
