package es.weso.simpleshex

import es.weso.rbe._
import es.weso.rbe.interval.IntervalChecker
import es.weso.collection.Bag
import es.weso.rbe.interval.IntOrUnbounded
import cats._
import cats.data._
import cats.data.NonEmptySet
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
    fromLabel: ShapeLabel,
    closed: Boolean,
    extra: List[PropertyId]
   ): Either[Reason, Set[ShapeLabel]] = 
   if (entity == null) Left(NullEntity(fromLabel))
   else {
    val cl: Either[Reason,Set[ShapeLabel]] = this match {
     case tc: TripleConstraint => {
       tc.checkLocalOpen(entity,fromLabel) match {
         case Left(err) => Left(err)
         case Right(Left(s)) => Right(s)
         case Right(Right((p,matches,failed))) => 
          if (failed == 0 || extra.contains(p)) Right(Set())
          else Left(NotAllowedNotInExtra(List((p,failed))))
       }
     } 
     case EachOf(Nil) => Right(Set())
     case EachOf(ts) => {
        val results = ts.map(_.checkLocalOpen(entity,fromLabel)).sequence.map(_.sequence)
        results match {
         case Left(err) => Left(err)
         case Right(e) => e match {
            case Left(s) => Right(s)
            case Right(tuples) => {
              val withErrors = tuples.collect { case (p, _, failed) if failed > 0 => (p,failed)}
              // Not allowed are triples that failed and that are not in EXTRA
              val notAllowed = withErrors.filter { case (p,failed) => !extra.contains(p) } 
              if (notAllowed.nonEmpty) Left(NotAllowedNotInExtra(notAllowed))
              else Right(Set())
            }
          }
        }
      }
     case OneOf(Nil) => Right(Set())
     case _ => Left(NotImplemented("checkLocal EachOf"))
      /* case OneOf(ts) => 
        combineChecks(ts.map(t => t.checkLocalOpen(entity,fromLabel))
      )*/ 
   }
   /* println(s"""|checkLocal($entity,$fromLabel,$this)=
               |$cl
               |""".stripMargin) */
   cl
  }

/*  private def combineChecks(
    cs: List[Either[Reason, Either[Set[ShapeLabel], (PropertyId, Int, Int)]]]): 
    Either[Reason,Set[ShapeLabel]] = {
    val (errs, lss) = cs.separate
    if (errs.isEmpty) {
      Right(lss.toSet.flatten)
    } else 
      Left(ErrorsMatching(errs))
  } */

}

case class EachOf(exprs: List[TripleConstraint]) extends TripleExpr 
case class OneOf(exprs: List[TripleConstraint]) extends TripleExpr 

sealed abstract class TripleConstraint extends TripleExpr with Serializable with Product {
  def min: Int
  def max: IntOrUnbounded
  def property: PropertyId

  /**
    * Checks local statements of an entity allowing extra values
    *
    * @param entity
    * @param fromLabel
    * @return either an error or either a set of pendingLabels or a list of (property, matches, failed) values
    */
  def checkLocalOpen(
    entity: Entity, 
    fromLabel: ShapeLabel
    ): Either[Reason, Either[Set[ShapeLabel], (PropertyId, Int,Int)]] = 
    this match {
       case tr: TripleConstraintRef => Right(Left(Set(fromLabel)))
       case tl: TripleConstraintLocal => {
        val found = entity.localStatementsByPropId(tl.property)
        val matches: Int = 
          found.map(s => tl.value.matchLocal(s.literal)).collect { case Right(()) => () }.size
        val failed = found.size - matches
        if (min <= matches && max >= matches) Right(Right(tl.property,matches,failed))
         else Left(CardinalityError(tl.property,matches,min,max))
       }
    }
}

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