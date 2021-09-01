package es.weso.wdsub.spark.pschema

import es.weso.collection.Bag
import cats.implicits._

// case class VTriple[VD,P,L](value: VD, p: P, label:L)

case class Msg[VD, L, E, P: Ordering](
    validate: Set[L], 
    validated: Set[(L,(VD, P, L))], 
    notValidated: Set[(L,(VD, P, L), Set[E])], 
    waitFor: Set[(L,(VD,P,L))]
    ) extends Serializable {

    def withValidate(v: Set[L]): Msg[VD,L,E, P] = 
      this.copy(validate = v)  
    def withValidated(v: Set[(L,(VD,P,L))]): Msg[VD,L,E, P] = 
      this.copy(validated = v)  
    def withNotValidated(v: Set[(L,(VD,P,L),Set[E])]): Msg[VD, L, E, P] = 
      this.copy(notValidated = v)  
    def withWaitFor(w: Set[(L,(VD,P,L))]): Msg[VD, L, E, P] = 
      this.copy(waitFor = w)  

    def merge(other: Msg[VD, L, E, P]): Msg[VD, L, E, P] = {
      Msg(
        validate = this.validate.union(other.validate),
        validated = this.validated ++ other.validated,
        notValidated = this.notValidated ++ other.notValidated,
        waitFor = this.waitFor ++ other.waitFor
      )
    }

    override def toString = s"Msg = ${
      if (validate.isEmpty) "" else "Validate: " + validate.map(_.toString).mkString(",")
    }${
      if (validated.isEmpty) "" else "Validated: " + validated.map(_.toString).mkString(",")
      }${
      if (waitFor.isEmpty) "" else "WaitFor: " + waitFor.map(_.toString).mkString(",")
      }${
      if (notValidated.isEmpty) "" else "NotValidated: " + notValidated.map(_.toString).mkString(",")
      }"
  } 

object Msg {

  def empty[VD,L,E, P: Ordering]: Msg[VD,L,E, P] = 
    Msg(Set[L](), 
    Set[(L, (VD, P, L))](), 
    Set[(L, (VD, P, L), Set[E])](),
    Set[(L, (VD, P, L))]())

  def validate[VD, L, E, P: Ordering](
    shapes: Set[L]
    ): Msg[VD, L, E, P] = 
      empty.withValidate(shapes)

  def waitFor[VD, L, E, P:Ordering](srcShape: L, p:P, dstShape: L, dst: VD): Msg[VD,L,E,P] = 
      empty.withWaitFor(Set((srcShape,(dst, p,dstShape))))
  
  def validated[VD, L, E, P:Ordering](l: L, p: P, lbl: L, v: VD): Msg[VD,L,E,P] =
      empty.withValidated(Set((l,(v,p,lbl))))

  def notValidated[VD, L,E, P:Ordering](l: L, p: P, lbl: L, v: VD, es: Set[E]) =
      empty.withNotValidated(Set((l, (v, p,lbl), es)))

}
