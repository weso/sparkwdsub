package es.weso.simpleshex

import es.weso.rbe._
import es.weso.rbe.interval.IntervalChecker
import es.weso.collection.Bag
import es.weso.rbe.interval.IntOrUnbounded

  sealed abstract class ShapeExpr extends Product with Serializable {
    def dependsOn(): Set[ShapeLabel] = this match {
      case s: ShapeRef => Set(s.label)
      case t: TripleConstraint => t.value.dependsOn
      case eo: EachOf => eo.exprs.foldLeft(Set[ShapeLabel]()){ case (e,s) => e.union(s.dependsOn) }
      case oo: OneOf => oo.exprs.foldLeft(Set[ShapeLabel]()){ case (e,s) => e.union(s.dependsOn) }
      case e: NodeConstraint => Set()
      case EmptyExpr => Set() 
    }

    def rbe: Rbe[Value.PropertyId] = this match {
      case _: ShapeRef => Empty 
      case t: TripleConstraint => Symbol(t.property, t.min, t.max)
      case eo: EachOf => {
        val empty: Rbe[Value.PropertyId] = Empty
        eo.exprs.foldLeft(empty){ case (e,b) => And(e,b.rbe)}
      }
      case oo: OneOf => {
        val empty: Rbe[Value.PropertyId] = Empty
        oo.exprs.foldLeft(empty){ case (e,b) => Or(e,b.rbe)}
      }
      case _: NodeConstraint => Empty
      case EmptyExpr => Empty
    }

    private lazy val checker = IntervalChecker(rbe)

    val tripleConstraints: List[TripleConstraint] = this match {
      case _: ShapeRef => List()
      case t: TripleConstraint => List(t) 
      case eo: EachOf => eo.exprs.map(_.tripleConstraints).flatten
      case oo: OneOf => oo.exprs.map(_.tripleConstraints).flatten
      case _: NodeConstraint => List()
      case _ => List()
    }

    def checkNeighs(bag: Bag[Value.PropertyId]): Either[Reason, Unit] =
       checker.check(bag,true) match {
         case Left(es) => Left(NoMatch(bag,rbe,es))
         case Right(_) => Right(())
       } 

    def checkLocal(value: Value, fromLabel: ShapeLabel): Either[Reason, Set[ShapeLabel]] =
     this match {
      case ShapeRef(label) => Right(Set(label))
      case EmptyExpr => Right(Set())
      case TripleConstraint(_,_,_,_) => Right(Set(fromLabel))
      case EachOf(Nil) => Right(Set())
      case EachOf(_) => Right(Set(fromLabel))
      case OneOf(Nil) => Right(Set())
      case OneOf(_) => Right(Set(fromLabel))
      case ValueSet(vs) => value match {
        case e: Entity => if (vs.contains(e.id)) Right(Set())
        else Left(NoValueValueSet(value,vs))
        case _ => Left(NoValueValueSet(value,vs))
      }
      case StringDatatype => value match {
        case _: StringValue => Right(Set())
        case _ => Left(NoStringDatatype(value, fromLabel))
      }
     }
   
  }

case class ShapeRef(label: ShapeLabel) extends ShapeExpr 
case class TripleConstraint(property: String, value: ShapeRef, min: Int, max: IntOrUnbounded) extends ShapeExpr 

sealed abstract class TripleExpr extends ShapeExpr with Product with Serializable
case class EachOf(exprs: List[TripleConstraint]) extends TripleExpr 
case class OneOf(exprs: List[TripleConstraint]) extends TripleExpr 
case object EmptyExpr extends TripleExpr 
sealed abstract class NodeConstraint extends ShapeExpr 
case class ValueSet(values: Set[String]) extends NodeConstraint
case object StringDatatype extends NodeConstraint
