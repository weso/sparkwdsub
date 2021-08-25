package es.weso.simpleshex
import es.weso.graphxhelpers._
import es.weso.graphxhelpers.GraphBuilder._
import es.weso.rdf.nodes._
import org.apache.spark.graphx.Edge

sealed abstract class Value extends Product with Serializable {
    val vertexId: Long
}

case class Entity(
    id: String, 
    vertexId: Long, 
    label: String, 
    siteIri: String = Value.siteDefault
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
    siteIri: String = Value.siteDefault
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


object Value {

  type PropertyId = String 
    
  def vertexEdges(
   triplets: List[(Entity, Property, Value, List[Qualifier])]
   ):(Seq[Vertex[Value]], Seq[Edge[Property]]) = {
    val subjects: Seq[Value] = triplets.map(_._1)
    val objects: Seq[Value] = triplets.map(_._3)
    val properties: Seq[Value] = triplets.map(_._2)
    val qualProperties: Seq[Value] = triplets.map(_._4.map(_.property)).flatten
    val qualValues: Seq[Value] = triplets.map(_._4.map(_.value)).flatten
    val values: Seq[Vertex[Value]] = subjects.union(objects).union(properties).union(qualProperties).union(qualValues).map(v => Vertex(v.vertexId,v))
    val edges = triplets.map(t => statement(t._1, t._2, t._3, t._4)).toSeq
    (values,edges)
  }

  def triple(subj: Entity, prop: Property, value: Value): (Entity, Property, Value, List[Qualifier]) = {
    (subj, prop, value, List())
  }

  def tripleq(subj: Entity, prop: Property, value: Value, qs: List[Qualifier]): (Entity, Property, Value, List[Qualifier]) = {
    (subj, prop, value, qs)
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

  def Str(str: String): Builder[StringValue] = for {
      id <- getIdUpdate
  } yield StringValue(str, id)

  def statement(
    subject: Entity, 
    property: Property, 
    value: Value, 
    qs: List[Qualifier]): Edge[Property] = 
      Edge(subject.vertexId, value.vertexId, property.withQualifiers(qs.toList))

  val siteDefault = "http://www.wikidata.org/entity"
    
}