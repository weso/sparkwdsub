package es.weso.simpleshex

import es.weso.rbe._
import es.weso.rbe.interval.IntervalChecker
import es.weso.collection.Bag
import es.weso.rbe.interval.IntOrUnbounded
import cats._
import cats.implicits._
import es.weso.wbmodel._
import es.weso.rdf.nodes._

sealed abstract class ShapeExpr extends Product with Serializable {

 def dependsOn(): Set[ShapeLabel] = this match {
   case s: ShapeRef => Set(s.label)
   case s: Shape => s.expression.dependsOn
   case e: NodeConstraint => Set()
   case EmptyExpr => Set() 
   case sand: ShapeAnd => sand.exprs.map(_.dependsOn).toSet.flatten
   case sor: ShapeOr => sor.exprs.map(_.dependsOn).toSet.flatten
   case sn: ShapeNot => sn.shapeExpr.dependsOn
 }

 lazy val empty: Rbe[(PropertyId,ShapeLabel)] = Empty 

 def rbe: Rbe[(PropertyId,ShapeLabel)] = { 
  this match {
    case _: ShapeRef => empty
    case _: NodeConstraint => empty
    case Shape(_,tripleExpr) => tripleExpr.rbe
    case EmptyExpr => empty
    case _ => empty // TODO!!
   }
  }

  implicit val showPair: Show[(PropertyId, ShapeLabel)] = Show.show(p => p.toString)

  private lazy val checker = IntervalChecker(rbe)

  val tripleConstraints: List[TripleConstraintRef] = this match {
    case _: ShapeRef => List()
    case Shape(_,tripleExpr) => tripleExpr match {
      case t: TripleConstraintRef => List(t) 
      case t: TripleConstraintLocal => List()
      case eo: EachOf => eo.exprs.map(_.tripleConstraints).flatten
      case oo: OneOf => oo.exprs.map(_.tripleConstraints).flatten
      case _ => List()
    }
      // tripleExpr.tripleConstraints
    case _: NodeConstraint => List()
    case _ => List()
  }

  def checkNeighs(bag: Bag[(PropertyId,ShapeLabel)]): Either[Reason, Unit] =
     checker.check(bag,true) match {
       case Left(es) => Left(NoMatch(bag,rbe,es))
       case Right(_) => Right(())
     } 

  def checkLocal(
    entity: Entity, 
    fromLabel: ShapeLabel
   ): Either[Reason, Set[ShapeLabel]] = {
    val result: Either[Reason,Set[ShapeLabel]] = this match {
     case ShapeRef(label) => Right(Set(label))
     case Shape(_,te) => te.checkLocal(entity, fromLabel)
      case vs: ValueSet => vs.matchLocal(entity).map(_ => Set()) 
      case StringDatatype => entity match {
//        case _: StringValue => Right(Set())
        case _ => Left(NoStringDatatype(entity))
      }
      case EmptyExpr => Right(Set())
      // TODO: ShapeAnd, ShapeOR, ShapeNot...
      case _ => Right(Set())
     }
     // println(s"checkLocal(se: ${this}, entity: $entity,fromLabel: $fromLabel)=${result}")
     result
  }   


 }

case class ShapeAnd(id: Option[ShapeLabel], exprs: List[ShapeExpr]) extends ShapeExpr
case class ShapeOr(id: Option[ShapeLabel], exprs: List[ShapeExpr]) extends ShapeExpr
case class ShapeNot(id: Option[ShapeLabel], shapeExpr: ShapeExpr) extends ShapeExpr
case class ShapeRef(label: ShapeLabel) extends ShapeExpr 
case class Shape(id: Option[ShapeLabel], expression: TripleExpr) extends ShapeExpr 
case object EmptyExpr extends ShapeExpr

sealed abstract class NodeConstraint extends ShapeExpr {
  def matchLocal(value: Value): Either[Reason, Unit]
}


case class ValueSet(id: Option[ShapeLabel], values: List[ValueSetValue]) extends NodeConstraint {
  override def matchLocal(value: Value) = {
    val found = value match {
     case e: Entity => values.collect { case i: IRIValue => i.iri }.contains(e.entityId.iri)
     case e: EntityId => values.collect { case i: IRIValue => i.iri }.contains(e.iri)
//     case s: StringValue => values.collect { case s: StringValue => s.str }.contains(s)
     case _ => false
    }
    if (found) Right(())
    else Left(NoValueValueSet(value,values))
  }
}


case object StringDatatype extends NodeConstraint {
  override def matchLocal(value: Value) = value match {
    case _: StringValue => ().asRight
    case _ => NoStringDatatype(value).asLeft
  }
}

sealed trait ValueSetValue 
sealed trait ObjectValue extends ValueSetValue
case class IRIValue(iri: IRI) extends ObjectValue
