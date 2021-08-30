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
   case s: Shape => s.expression match {
     case None => Set()
     case Some(te) => te.dependsOn()
   }
   case e: NodeConstraint => Set()
   case sand: ShapeAnd => sand.exprs.map(_.dependsOn).toSet.flatten
   case sor: ShapeOr => sor.exprs.map(_.dependsOn).toSet.flatten
   case sn: ShapeNot => sn.shapeExpr.dependsOn
 }

 lazy val empty: Rbe[(PropertyId,ShapeLabel)] = Empty 

 def rbe: Rbe[(PropertyId,ShapeLabel)] = { 
  this match {
    case _: ShapeRef => empty
    case _: NodeConstraint => empty
    case s: Shape => s.expression match {
      case None => empty
      case Some(te) => 
        // TODO: Extend with extras?
        te.rbe
    }
    case _ => empty // TODO!!
   }
  }

  implicit val showPair: Show[(PropertyId, ShapeLabel)] = Show.show(p => p.toString)

  private lazy val checker = IntervalChecker(rbe)

  def tripleConstraints(schema: Schema): List[TripleConstraintRef] = 
   this match {
    case sr: ShapeRef => schema.get(sr.label) match {
      case None => List()
      case Some(se) => se.tripleConstraints(schema)
    }
    case s: Shape => s.expression match {
      case None => List()
      case Some(te) => te match {
       case t: TripleConstraintRef => List(t) 
       case t: TripleConstraintLocal => List()
       case eo: EachOf => eo.exprs.map(_.tripleConstraints).flatten
       case oo: OneOf => oo.exprs.map(_.tripleConstraints).flatten
       case _ => List()
      }
    }
    case _: NodeConstraint => List()
    case _ => List()
  }

  def checkNeighs(
    bag: Bag[(PropertyId,ShapeLabel)],
    failed: Set[(PropertyId, ShapeLabel)],
    schema: Schema
    ): Either[Reason, Unit] =
    this match {
      case s: Shape => {
       checker.check(bag,s.closed) match {
        case Left(es) => Left(NoMatch(bag,rbe,es))
        case Right(_) => {
          // check that all failed properties are in Extra
          val failedPropsNotExtra = failed.filter{ case (p,_) => !s.extra.contains(p) }
          if (failedPropsNotExtra.nonEmpty) {
           Left(FailedPropsNotExtra(failedPropsNotExtra))
          } else Right(())
        }
       } 
      }
      case ns: ShapeNot => {
        ns.shapeExpr.checkNeighs(bag, failed, schema) match {
         case Left(_) => Right(())
         case Right(_) => Left(MatchNot(bag, rbe))
        }
      }
      case sa: ShapeAnd => {
        val es = sa.exprs.map(_.checkNeighs(bag, failed, schema))
        val errs = es.collect { case Left(err) => err }
        if (errs.nonEmpty) Left(ErrorsMatching(errs))
        else Right(())
      }
      case so: ShapeOr => {
        // TODO: Review case where neighs pass with one shape and fail locally with another, 
        // but the opposite happens and the overall result passes...
        val es = so.exprs.map(_.checkNeighs(bag, failed, schema))
        if (es.filter(_.isRight).nonEmpty) Right(())
        else Left(ShapeOr_AllFailed(es.collect{ case Left(err) => err}))
      }
      case nc: NodeConstraint => Right(())
      case sr: ShapeRef => schema.get(sr.label) match {
        case None => Left(ShapeNotFound(sr.label, schema))
        case Some(se) => se.checkNeighs(bag,failed,schema)
      }
    }

  def checkLocal(
    entity: Entity, 
    fromLabel: ShapeLabel,
    schema: Schema
   ): Either[Reason, Set[ShapeLabel]] = {
    val result: Either[Reason,Set[ShapeLabel]] = this match {
    case ShapeRef(label) => schema.get(label) match {
      case None => Left(ShapeNotFound(label, schema))
      case Some(se) => {
        se.checkLocal(entity,fromLabel,schema)
      }
    }
    case s: Shape => s.expression match {
     case None => Right(Set())
     case Some(te) => {
      /* println(s"""|CheckLocal................
                  |TripleExpr: $te
                  |""".stripMargin)   */
      te.checkLocal(entity, fromLabel, s.closed, s.extra)
     }
      
    }  
    case vs: ValueSet => vs.matchLocal(entity).map(_ => Set()) 
    case StringDatatype => entity match {
//        case _: StringValue => Right(Set())
        case _ => Left(NoStringDatatype(entity))
      }
      case EmptyExpr => Right(Set())
      // TODO: ShapeAnd, ShapeOR, ShapeNot...
      case _ => Right(Set())
     }
/*     println(s"""|checkLocal with
                 | se: ${this}
                 | entity: $entity
                 | entity local statememts: ${entity.localStatements.mkString(",")}
                 | fromLabel: $fromLabel
                 | result: ${result}
                 |""".stripMargin) */
     result
  }   
 }

case class ShapeAnd(
  id: Option[ShapeLabel], 
  exprs: List[ShapeExpr]) extends ShapeExpr
case class ShapeOr(
  id: Option[ShapeLabel], 
  exprs: List[ShapeExpr]) extends ShapeExpr
case class ShapeNot(
  id: Option[ShapeLabel], 
  shapeExpr: ShapeExpr) extends ShapeExpr
case class ShapeRef(
  label: ShapeLabel
  ) extends ShapeExpr 
case class Shape(
  id: Option[ShapeLabel], 
  closed: Boolean,
  extra: List[PropertyId], // TODO: Extend extras to handle Paths?
  expression: Option[TripleExpr]
) extends ShapeExpr 

sealed abstract class NodeConstraint extends ShapeExpr {
  def matchLocal(value: Value): Either[Reason, Unit]
}

case object EmptyExpr extends NodeConstraint {
  override def matchLocal(
    value: Value
    ): Either[Reason,Unit] = Right(())
}

case class ValueSet(id: Option[ShapeLabel], values: List[ValueSetValue]) extends NodeConstraint {
  override def matchLocal(value: Value) = {
    val found = value match {
     case e: Entity => values.collect { case ve: EntityIdValueSetValue => ve.id }.contains(e.entityId)
     case e: EntityId => values.collect { case ve: EntityIdValueSetValue => ve.id }.contains(e)
     case i: IRIValue => values.collect { case iv: IRIValueSetValue => iv.iri }.contains(i.iri)
     case s: StringValue => values.collect { case s: StringValueSetValue => s.str }.contains(s.str)
     case _ => false
    }
    if (found) Right(())
    else Left(NoValueValueSet(value,values))
  }
}


case object StringDatatype extends NodeConstraint {
  override def matchLocal(value: Value) = {
    val result = value match {
    case _: StringValue => ().asRight
    case _ => NoStringDatatype(value).asLeft
  }
 /* println(s"""|matchLocal
              |nodeConstraint: $this
              |Value: $value
              |Valuetype: ${value.getClass().getCanonicalName()}
              |Result: $result
              |""".stripMargin) */
  result
 }
}

sealed trait ValueSetValue 
sealed trait NonLocalValueSetValue extends ValueSetValue
sealed trait LocalValueSetValue extends ValueSetValue

case class EntityIdValueSetValue(id: EntityId) extends NonLocalValueSetValue
case class IRIValueSetValue(iri: IRI) extends LocalValueSetValue
case class StringValueSetValue(str: String) extends LocalValueSetValue
