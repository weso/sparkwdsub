package es.weso.simpleshex

import es.weso.rbe._
import es.weso.rbe.interval.IntervalChecker
import es.weso.collection.Bag
import es.weso.rbe.interval.IntOrUnbounded
import cats._
import cats.implicits._

  sealed abstract class ShapeExpr extends Product with Serializable {

    def dependsOn(): Set[ShapeLabel] = this match {
      case s: ShapeRef => Set(s.label)
      case t: TripleConstraintRef => t.value.dependsOn
      case t: TripleConstraintLocal => Set()
      case eo: EachOf => eo.exprs.foldLeft(Set[ShapeLabel]()){ case (e,s) => e.union(s.dependsOn) }
      case oo: OneOf => oo.exprs.foldLeft(Set[ShapeLabel]()){ case (e,s) => e.union(s.dependsOn) }
      case e: NodeConstraint => Set()
      case EmptyExpr => Set() 
    }

    def rbe: Rbe[(PropertyId,ShapeLabel)] = { 
     lazy val empty: Rbe[(PropertyId,ShapeLabel)] = Empty 
     this match {
      case _: ShapeRef => empty
      case t: TripleConstraintRef => Symbol((t.property, t.value.label), t.min, t.max)
      case t: TripleConstraintLocal => empty
      case eo: EachOf => 
        eo.exprs.foldLeft(empty){ case (e,b) => And(e,b.rbe)}
      case oo: OneOf => 
        oo.exprs.foldLeft(empty){ case (e,b) => Or(e,b.rbe)}
      case _: NodeConstraint => empty
      case EmptyExpr => empty
     }
    }

    implicit val showPair: Show[(PropertyId, ShapeLabel)] = Show.show(p => p.toString)

    private lazy val checker = IntervalChecker(rbe)

    val tripleConstraints: List[TripleConstraintRef] = this match {
      case _: ShapeRef => List()
      case t: TripleConstraintRef => List(t) 
      case t: TripleConstraintLocal => List()
      case eo: EachOf => eo.exprs.map(_.tripleConstraints).flatten
      case oo: OneOf => oo.exprs.map(_.tripleConstraints).flatten
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
      case EmptyExpr => Right(Set())
      case TripleConstraintRef(_,_,_,_) => Right(Set(fromLabel))
      case TripleConstraintLocal(p,nc,min,max) => {
        val found = entity.localStatements.filter(_.propertyRecord.id == p)
        val dontMatch: List[Reason] = 
          found.map(s => nc.matchLocal(s.literal)).collect { case Left(err) => err }
        if (!dontMatch.isEmpty) Left(ErrorsMatching(dontMatch))
        else {
          val count = found.size
          if (min <= count && max >= count) Right(Set())
          else Left(CardinalityError(count,min,max))
        }
      }
        
      case EachOf(Nil) => Right(Set())
      case EachOf(ts) => combineChecks(ts.map(t => t.checkLocal(entity,fromLabel)))
      case OneOf(Nil) => Right(Set())
      case OneOf(ts) => combineChecks(ts.map(t => t.checkLocal(entity,fromLabel)))
      case ValueSet(vs) => entity match {
        case e: Item => if (vs.contains(e.entityId)) Right(Set())
        else Left(NoValueValueSet(entity,vs))
        case _ => Left(NoValueValueSet(entity,vs))
      }
      case StringDatatype => entity match {
//        case _: StringValue => Right(Set())
        case _ => Left(NoStringDatatype(entity))
      }
     }
     // println(s"checkLocal(se: ${this}, entity: $entity,fromLabel: $fromLabel)=${result}")
     result
    }   

    private def combineChecks(cs: List[Either[Reason, Set[ShapeLabel]]]): 
      Either[Reason,Set[ShapeLabel]] = {
      val (errs, lss) = cs.separate
      if (errs.isEmpty) {
        Right(lss.toSet.flatten)
      } else 
        Left(ErrorsMatching(errs))
    }

  }

case class ShapeRef(label: ShapeLabel) extends ShapeExpr 
sealed abstract class TripleConstraint extends ShapeExpr
case class TripleConstraintRef(
  property: PropertyId, 
  value: ShapeRef, 
  min: Int, 
  max: IntOrUnbounded
  ) extends TripleConstraint
case class TripleConstraintLocal(
  property: PropertyId, 
  value: NodeConstraint, 
  min: Int, 
  max: IntOrUnbounded
  ) extends TripleConstraint

sealed abstract class TripleExpr extends ShapeExpr with Product with Serializable
case class EachOf(exprs: List[TripleConstraint]) extends TripleExpr 
case class OneOf(exprs: List[TripleConstraint]) extends TripleExpr 
case object EmptyExpr extends TripleExpr 
sealed abstract class NodeConstraint extends ShapeExpr {
  def matchLocal(value: Value): Either[Reason, Unit]
}
case class ValueSet(values: Set[Value]) extends NodeConstraint {
  override def matchLocal(value: Value) = 
    if (values contains value) Right(())
    else Left(NoValueValueSet(value,values))

}
case object StringDatatype extends NodeConstraint {
  override def matchLocal(value: Value) = value match {
    case _: StringValue => ().asRight
    case _ => NoStringDatatype(value).asLeft
  }
}
