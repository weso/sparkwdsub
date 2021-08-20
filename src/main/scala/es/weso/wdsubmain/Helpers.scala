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

object Helpers {

  case class ShapedValue(
    value: Value, 
    shapesInfo: ShapesInfo = ShapesInfo()
  ) {

    def replaceShapeBy(shape1:ShapeLabel, shape2: ShapeLabel): ShapedValue = this.copy(shapesInfo = this.shapesInfo.replaceShapeBy(shape1,shape2))
    def addOKShape(shape: ShapeLabel): ShapedValue = this.copy(shapesInfo = this.shapesInfo.addOkShape(shape))
    def addNoShape(shape: ShapeLabel, reason: Reason): ShapedValue = this.copy(shapesInfo = this.shapesInfo.addNoShape(shape, reason))
  }

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

  sealed abstract class Reason
  case class NoValueForProperty(prop: Property) extends Reason
  case class ValueIsNot(expectedId: String) extends Reason

  case class ShapesInfo(
    pendingShapes: Set[ShapeLabel] = Set(), 
    okShapes: Set[ShapeLabel] = Set(), 
    noShapes: Set[ShapeLabel] = Set(),
    inconsistencies: Set[ShapeLabel] = Set()
    ) {

    def replaceShapeBy(shape1: ShapeLabel, shape2: ShapeLabel) = 
      this.copy(pendingShapes = (this.pendingShapes - (shape1) + (shape2)))

    def withPendingShapes(shapes: Set[ShapeLabel]): ShapesInfo =
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
  }

  object ShapesInfo {
    lazy val default: ShapesInfo = ShapesInfo(Set(),Set(),Set())
  }

  case class Entity(
    id: String, 
    vertexId: Long, 
    label: String, 
    siteIri: String = siteDefault,
    shapesInfo: ShapesInfo = ShapesInfo.default
    ) extends Value {
    def iri: IRI = IRI(siteIri + "/" + id)
    override def toString = s"$id-$label@$vertexId||{${shapesInfo.pendingShapes.map(_.name).mkString(",")}}"
  }

  case class StringValue(
    str: String, 
    vertexId: Long, 
    shapesInfo: ShapesInfo = ShapesInfo.default
    ) extends Value {
    override def toString = s"$str@$vertexId"
  }

  case class DateValue(
    date: String, 
    vertexId: Long, 
    shapesInfo: ShapesInfo = ShapesInfo.default
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
    siteIri: String = siteDefault,
    shapesInfo: ShapesInfo = ShapesInfo.default
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