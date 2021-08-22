package es.weso.sparkwdsub

import es.weso.wikibase._
import org.wikidata.wdtk.datamodel.implementation._
import org.wikidata.wdtk.datamodel.helpers.ItemDocumentBuilder
import org.wikidata.wdtk.datamodel.interfaces.EntityDocument
import org.wikidata.wdtk.datamodel.helpers.StatementBuilder
import org.wikidata.wdtk.datamodel.interfaces.Value;
import org.wikidata.wdtk.datamodel.interfaces._
import es.weso.rdf.nodes.IRI
import cats.data._ 
import cats.implicits._
// import es.weso.wshex._
import org.apache.spark.graphx.Edge
import es.weso.rbe.interval.IntOrUnbounded
import es.weso.rbe.interval.IntLimit
import es.weso.rbe.{Graph => _, _}
import es.weso.rbe.interval.IntervalChecker
import es.weso.collection.Bag
import es.weso.shex
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import es.weso.pschema.GraphBuilder._
import es.weso.pschema.Vertex

object Helpers {

  case class ShapeLabel(name: String) extends Serializable {
    override def toString: String = name
  }

  val siteDefault = "http://www.wikidata.org/entity"


  sealed abstract class Value extends Product with Serializable {
    val vertexId: Long
  }

  sealed abstract class Reason extends Product with Serializable
  case class NoValueForProperty(prop: Property) extends Reason
  case class ValueIsNot(expectedId: String) extends Reason
  case class ShapeNotFound(shapeLabel: ShapeLabel, schema: Schema) extends Reason
  case class NoMatch(bag: Bag[PropertyId], rbe: Rbe[PropertyId], errors: NonEmptyList[RbeError]) extends Reason
  case class NoValueValueSet(value: Value, valueSet: Set[String]) extends Reason

  type PropertyId = String 

  case class Schema(map: Map[ShapeLabel, ShapeExpr]) extends Serializable {

    def get(shapeLabel: ShapeLabel): Option[ShapeExpr] = 
      map.get(shapeLabel)

/*    def getTripleConstraints(shapeLabel: ShapeLabel): List[TripleConstraint] = {
      get(shapeLabel) match {
        case None => List()
        case Some(se) => se.tripleConstraints
      }
    } */  

    def checkLocal(label: ShapeLabel, value: Value): Either[Reason, Set[ShapeLabel]] = {
      get(label) match {
        case None => Left(ShapeNotFound(label,this))
        case Some(se) => se.checkLocal(value)
      }
    }

    def checkNeighs(label: ShapeLabel, neighs: Bag[PropertyId]): Either[Reason, Unit] = {
      get(label) match {
        case None => Left(ShapeNotFound(label,this))
        case Some(se) => se.checkNeighs(neighs)
      }
    }

    def getTripleConstraints(label: ShapeLabel): List[(PropertyId, ShapeLabel)] = {
      get(label) match {
        case None => List()
        case Some(se) => se.tripleConstraints.map(tc => (tc.property, tc.value.label))
      }
    }


  }

  sealed abstract class ShapeExpr extends Product with Serializable {
    def dependsOn(): Set[ShapeLabel] = this match {
      case s: ShapeRef => Set(s.label)
      case t: TripleConstraint => t.value.dependsOn
      case eo: EachOf => eo.exprs.foldLeft(Set[ShapeLabel]()){ case (e,s) => e.union(s.dependsOn) }
      case e: NodeConstraint => Set()
      case EmptyExpr => Set() 
    }

    def rbe: Rbe[PropertyId] = this match {
      case _: ShapeRef => Empty 
      case t: TripleConstraint => Symbol(t.property, t.min, t.max)
      case eo: EachOf => {
        val empty: Rbe[PropertyId] = Empty
        eo.exprs.foldLeft(empty){ case (e,b) => And(e,b.rbe)}
      }
      case _: NodeConstraint => Empty
      case EmptyExpr => Empty
    }

    private lazy val checker = IntervalChecker(rbe)

    val tripleConstraints: List[TripleConstraint] = this match {
      case _: ShapeRef => List()
      case t: TripleConstraint => List(t) 
      case eo: EachOf => eo.exprs.map(_.tripleConstraints).flatten
      case _: NodeConstraint => List()
      case _ => List()
    }

    def checkNeighs(bag: Bag[PropertyId]): Either[Reason, Unit] =
       checker.check(bag,true) match {
         case Left(es) => Left(NoMatch(bag,rbe,es))
         case Right(_) => Right(())
       } 

    def checkLocal(value: Value): Either[Reason, Set[ShapeLabel]] =
     this match {
      case ShapeRef(label) => Right(Set(label))
      case EmptyExpr => Right(Set())
      case TripleConstraint(_,_,_,_) => Right(Set())
      case EachOf(_) => Right(Set())
      case ValueSet(vs) => value match {
        case e: Entity => if (vs.contains(e.id)) Right(Set())
        else Left(NoValueValueSet(value,vs))
        case _ => Left(NoValueValueSet(value,vs))
      }
     }
   
  }

  case class ShapeRef(label: ShapeLabel) extends ShapeExpr 
  case class TripleConstraint(property: String, value: ShapeRef, min: Int, max: IntOrUnbounded) extends ShapeExpr 
  sealed abstract class TripleExpr extends ShapeExpr with Product with Serializable
  case class EachOf(exprs: List[TripleConstraint]) extends TripleExpr 
  case object EmptyExpr extends TripleExpr 
  sealed abstract class NodeConstraint extends ShapeExpr 
  case class ValueSet(values: Set[String]) extends NodeConstraint

  case class Entity(
    id: String, 
    vertexId: Long, 
    label: String, 
    siteIri: String = siteDefault
    ) extends Value {
    def iri: IRI = IRI(siteIri + "/" + id)
    override def toString = s"$id-$label@$vertexId"
  }

  case class StringValue(
    str: String, 
    vertexId: Long
    ) extends Value {
    override def toString = s"$str@$vertexId"
  }

  case class DateValue(
    date: String, 
    vertexId: Long
    ) extends Value {
    override def toString = s"$date@$vertexId"
  }

  case class Qualifier(property: Property, value: Value) {
    override def toString = s"$property:$value"
  }

  case class Property(
    id: String, 
    vertexId: Long,
    label: String,     
    qualifiers: List[Qualifier] = List(), 
    siteIri: String = siteDefault
    ) extends Value {
    def iri: IRI = IRI(siteIri + "/" + id)

    def withQualifiers(qs: List[Qualifier]): Property = /* 
     TODO[Doubt]: I'm not sure if we should update the id and generate a new one or keep the original id 
     for {
       id <- getIdUpdate
     } yield this.copy(vertexId = id, qualifiers = qs) */
     this.copy(qualifiers = qs)

    override def toString = s"$id - $label@$vertexId${if (qualifiers.isEmpty) "" else s"{{" + qualifiers.map(_.toString).mkString(",") + "}}" }" 

  } 

  object Property {
    implicit val orderingById: Ordering[Property] = Ordering.by(_.id)
  }

  def vertexEdges(triplets: (Entity, Property, Value, List[Qualifier])*):(Seq[Vertex[Value]], Seq[Edge[Property]]) = {
    val subjects: Seq[Value] = triplets.map(_._1)
    val objects: Seq[Value] = triplets.map(_._3)
    val properties: Seq[Value] = triplets.map(_._2)
    val qualProperties: Seq[Value] = triplets.map(_._4.map(_.property)).flatten
    val qualValues: Seq[Value] = triplets.map(_._4.map(_.value)).flatten
    val values: Seq[Vertex[Value]] = subjects.union(objects).union(properties).union(qualProperties).union(qualValues).map(v => Vertex(v.vertexId,v))
    val edges = triplets.map(t => statement(t._1, t._2, t._3, t._4)).toSeq
    (values,edges)
  }


  def Q(num: Int, label: String, site: String = siteDefault): Builder[Entity] =  for {
      id <- getIdUpdate
    } yield Entity("Q" + num, id, label)


    def P(num: Int, label: String, site: String = siteDefault): Builder[Property] = for {
      id <- getIdUpdate
    } yield Property("P" + num, id, label, List())


    def Date(date: String): Builder[DateValue] = for {
      id <- getIdUpdate
    } yield DateValue(date, id)


    def statement(subject: Entity, property: Property, value: Value, qs: List[Qualifier]): Edge[Property] = 
      Edge(subject.vertexId, value.vertexId, property.withQualifiers(qs.toList))


    
}