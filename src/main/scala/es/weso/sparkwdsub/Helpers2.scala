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
import es.weso.rbe._
import es.weso.rbe.interval.IntervalChecker
import es.weso.collection.Bag
import es.weso.shex

object Helpers2 {

  case class ShapeLabel(name: String) extends Serializable

  val siteDefault = "http://www.wikidata.org/entity"

  def build[A](builder: Builder[A]): A = builder.run(0L).map(_._2).value

  type Builder[A] = State[Long,A]
  def getIdUpdate: Builder[Long] = for {
    id <- State.get
    _ <- State.set(id + 1)
  } yield id

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

  case class Msg(
      validate: Set[ShapeLabel] = Set(), 
      outgoing: Set[PropertyId] = Set()) extends Serializable {

      def merge(other: Msg): Msg = {
        Msg(
          validate = this.validate.union(other.validate),
          outgoing = this.outgoing.union(other.outgoing)
        )
      }

      override def toString = s"Msg = ${
        if (validate.isEmpty) "" else "Validate: " + validate.map(_.toString).mkString(",")
      }${
        if (outgoing.isEmpty) "" else "Arcs: " + outgoing.map(_.toString).mkString(",")
      }"
    } 

  object Msg {
      def validate(shapes: Set[ShapeLabel]): Msg = Msg(validate = shapes, Set())
      def outgoing(arcs: Set[PropertyId]): Msg = Msg(Set(),arcs)
  }


  case class Schema(map: Map[ShapeLabel, ShapeExpr]) extends Serializable {

    def get(shapeLabel: ShapeLabel): Option[ShapeExpr] = 
      map.get(shapeLabel)

    def getTripleConstraints(shapeLabel: ShapeLabel): List[TripleConstraint] = {
      get(shapeLabel) match {
        case None => List()
        case Some(se) => se.tripleConstraints
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
  }

  case class ShapeRef(label: ShapeLabel) extends ShapeExpr 
  case class TripleConstraint(property: String, value: ShapeRef, min: Int, max: IntOrUnbounded) extends ShapeExpr 
  sealed abstract class TripleExpr extends ShapeExpr with Product with Serializable
  case class EachOf(exprs: List[TripleConstraint]) extends TripleExpr 
  case object EmptyExpr extends TripleExpr 
  sealed abstract class NodeConstraint extends ShapeExpr 
  case class ValueSet(values: Set[String]) extends NodeConstraint

  case class ShapedValue(
    value: Value, 
    shapesInfo: ShapesInfo = ShapesInfo.default,
    outgoing: Option[Bag[PropertyId]] = None
  ) extends Serializable {

    def addPendingShapes(shapes: Set[ShapeLabel]): ShapedValue =
      this.copy(shapesInfo = this.shapesInfo.addPendingShapes(shapes))
    def addOKShape(shape: ShapeLabel): ShapedValue = 
      this.copy(shapesInfo = this.shapesInfo.addOkShape(shape))
    def addNoShape(shape: ShapeLabel, err: Reason): ShapedValue = 
      this.copy(shapesInfo = this.shapesInfo.addNoShape(shape, err))

    def withOutgoing(bag: Bag[PropertyId]) = this.copy(outgoing = Some(bag))  
    def withoutPendingShapes = this.copy(shapesInfo = this.shapesInfo.withoutPendingShapes)

    def validatePendingShapes(schema: Schema, shapes: Set[ShapeLabel]): ShapedValue = 
      shapes.foldLeft(this){ case (v, shape) => v.validatePendingShape(schema, shape) }

    def validatePendingShape(schema: Schema, shape: ShapeLabel): ShapedValue =
      schema.get(shape) match {
        case None => addNoShape(shape, ShapeNotFound(shape, schema))
        case Some(ShapeRef(ref)) => validatePendingShape(schema, ref)
        case Some(TripleConstraint(_,_,_,_)) => addPendingShapes(Set(shape))
        case Some(EachOf(es)) => addPendingShapes(Set(shape))
        case Some(EmptyExpr) => addOKShape(shape)
        case Some(ValueSet(vs)) => this.value match {
          case e: Entity => 
            if (vs contains e.id) addOKShape(shape)
            else addNoShape(shape,NoValueValueSet(e, vs))
          case _ => addNoShape(shape,NoValueValueSet(this.value, vs))  
        }
      }

  }


  case class ShapesInfo(
    pendingShapes: Set[ShapeLabel] = Set(), 
    okShapes: Set[ShapeLabel] = Set(), 
    noShapes: Set[ShapeLabel] = Set(),
    inconsistencies: Set[ShapeLabel] = Set()
    ) {

    def replaceShapeBy(shape1: ShapeLabel, shape2: ShapeLabel) = 
      this.copy(pendingShapes = (this.pendingShapes - (shape1) + (shape2)))

    def addPendingShapes(shapes: Set[ShapeLabel]): ShapesInfo =
      this.copy(pendingShapes = this.pendingShapes ++ shapes)

    def addOkShape(shape: ShapeLabel) = 
      if (inconsistencies.contains(shape))
       this.copy(pendingShapes = this.pendingShapes - shape)
      else if (noShapes.contains(shape)) 
       this.copy(pendingShapes = this.pendingShapes - shape, inconsistencies = this.inconsistencies + shape)
      else 
        this.copy(pendingShapes = this.pendingShapes - shape, okShapes = this.okShapes + shape)
    
    def addNoShape(shape: ShapeLabel, reason: Reason) = 
      // TOOD: Do something with reason...
      if (inconsistencies.contains(shape)) 
        this.copy(pendingShapes = this.pendingShapes - shape)
      else if (okShapes.contains(shape)) 
        this.copy(pendingShapes = this.pendingShapes - shape, inconsistencies = this.inconsistencies + shape)
      else       
        this.copy(pendingShapes = this.pendingShapes - shape, noShapes = this.noShapes + shape)
 
        
    def withoutPendingShapes() = this.copy(pendingShapes = Set()) 
       
    private def showPendingShapes(): String = s"Pending:${if (pendingShapes.isEmpty) "{}" else pendingShapes.map(_.name).mkString(",")}"
    private def showOKShapes(): String = if (okShapes.isEmpty) "" else okShapes.map(_.name).mkString(",")
    private def showNoShapes(): String = if (noShapes.isEmpty) "" else noShapes.map(_.name).mkString(",")

    override def toString = showPendingShapes() + showOKShapes() + showNoShapes()
  
  }

  object ShapesInfo {
    lazy val default: ShapesInfo = ShapesInfo(Set(),Set(),Set())
  }

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

  def vertexEdges(triplets: (Entity, Property, Value, List[Qualifier])*):(Seq[Value], Seq[Edge[Property]]) = {
    val subjects: Seq[Value] = triplets.map(_._1)
    val objects: Seq[Value] = triplets.map(_._3)
    val properties: Seq[Value] = triplets.map(_._2)
    val qualProperties: Seq[Value] = triplets.map(_._4.map(_.property)).flatten
    val qualValues: Seq[Value] = triplets.map(_._4.map(_.value)).flatten
    val values: Seq[Value] = subjects.union(objects).union(properties).union(qualProperties).union(qualValues)
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

/*    def mkEntityValue(e: Entity): Value = {
      e.entityDocument.getEntityId()
    }
*/   

    type PassedShapes = Set[ShapeLabel]
    type PendingShapes = Set[ShapeLabel]
    sealed abstract class CheckingStatus
    case class Matching(pending: PendingShapes)
    case class NoMatching()

    /*
      <Researcher> {
        p31 [ Q5 ] ;
        p166 @<Award> * ;
        p19 @<Place> ;
      }
      <Place> {
        P18 @<Country>
      }
      <Country> { } 
     */ 
    def validateResearcher(entity: Entity): CheckingStatus = ???
    
}