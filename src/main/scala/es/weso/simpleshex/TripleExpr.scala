package es.weso.simpleshex

import es.weso.rbe._
import es.weso.rbe.interval.IntervalChecker
import es.weso.collection.Bag
import es.weso.rbe.interval.IntOrUnbounded
import cats._
import cats.implicits._
import es.weso.wbmodel._
import es.weso.rdf.nodes._

sealed abstract class TripleExpr extends Product with Serializable {

  lazy val empty: Rbe[(PropertyId,ShapeLabel)] = Empty 

  def dependsOn(): Set[ShapeLabel] = this match {
    case tcr: TripleConstraintRef => Set(tcr.value.label)
    case tcl: TripleConstraintLocal => Set()
    case eo: EachOf => eo.exprs.foldLeft(Set[ShapeLabel]()){ case (e,s) => e.union(s.dependsOn) }
    case oo: OneOf => oo.exprs.foldLeft(Set[ShapeLabel]()){ case (e,s) => e.union(s.dependsOn) }
    case _ => Set()  
  }

  def rbe: Rbe[(PropertyId,ShapeLabel)] = { 
  this match {
    case t: TripleConstraintRef => Symbol((t.property, t.value.label), t.min, t.max)
    case t: TripleConstraintLocal => empty
    case eo: EachOf => 
      eo.exprs.foldLeft(empty){ case (e,b) => And(e,b.rbe)}
    case oo: OneOf => 
      oo.exprs.foldLeft(empty){ case (e,b) => Or(e,b.rbe)}
   }
  }

  val tripleConstraints: List[TripleConstraintRef] = this match {
    case t: TripleConstraintRef => List(t) 
    case t: TripleConstraintLocal => List()
    case eo: EachOf => eo.exprs.map(_.tripleConstraints).flatten
    case oo: OneOf => oo.exprs.map(_.tripleConstraints).flatten
    case _ => List()
  }

  def checkLocal(
    entity: Entity, 
    fromLabel: ShapeLabel
   ): Either[Reason, Set[ShapeLabel]] = this match {
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

case class EachOf(exprs: List[TripleConstraint]) extends TripleExpr 
case class OneOf(exprs: List[TripleConstraint]) extends TripleExpr 

sealed abstract class TripleConstraint extends TripleExpr with Serializable with Product
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


