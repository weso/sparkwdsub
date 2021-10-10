package es.weso.wbmodel

import org.wikidata.wdtk.datamodel.helpers.JsonDeserializer
import org.apache.spark.graphx._
import DumpUtils._
import org.apache.spark.SparkContext
import java.nio.file.Path
import org.apache.spark.rdd.RDD
import org.wikidata.wdtk.datamodel.interfaces.{ 
  SiteLink => WBSiteLink,
  _
}
import collection.JavaConverters._
import org.wikidata.wdtk.datamodel.implementation.ItemDocumentImpl
import org.wikidata.wdtk.datamodel.implementation.PropertyDocumentImpl
import org.wikidata.wdtk.datamodel.implementation.DatatypeIdImpl
import org.wikidata.wdtk.datamodel.implementation.PropertyIdValueImpl
import org.wikidata.wdtk.datamodel.implementation.ItemIdValueImpl
import org.wikidata.wdtk.datamodel.implementation.MonolingualTextValueImpl
import org.wikidata.wdtk.datamodel.helpers.JsonSerializer
import java.io.ByteArrayOutputStream
import es.weso.simpleshex.ShapeLabel

object ValueWriter {

  def entity2JsonStr(v: Entity, showShapes: Boolean): String = {
    
    val os = new ByteArrayOutputStream()
//    val jsonSerializer = new JsonSerializer(os)
//    jsonSerializer.open()
    val str = entity2entityDocument(v) match {
      case id: ItemDocument => JsonSerializer.getJsonString(id) ++ printShapes(showShapes, v.okShapes)
      case pd: PropertyDocument => JsonSerializer.getJsonString(pd) ++ printShapes(showShapes, v.okShapes)
      case _ => ""
    }
    str
//    jsonSerializer.close()
  }

  private def printShapes(showShapes: Boolean, shapes: Set[ShapeLabel]): String = {
    if (showShapes && shapes.nonEmpty) {
      "// Shapes: " ++ shapes.map(_.name).mkString(",")
    } else ""
  }

  def entity2entityDocument(v: Entity): EntityDocument = v match {
    case i: Item => {
      val ed = new ItemDocumentImpl(
        cnvItemId(i.itemId), 
        cnvMultilingual(i.labels).asJava,
        cnvMultilingual(i.descriptions).asJava,
        cnvMultilingual(i.aliases).asJava,
        cnvStatements(i.localStatements).asJava,
        cnvSiteLinks(i.siteLinks).asJava,
        0L
        )
      ed  
    }
    case p: Property => {
      val pd = new PropertyDocumentImpl(
        cnvPropertyId(p.propertyId),
        cnvMultilingual(p.labels).asJava,
        cnvMultilingual(p.descriptions).asJava,
        cnvMultilingual(p.aliases).asJava,
        cnvStatements(p.localStatements).asJava,
        cnvDatatype(p.datatype),
        0L
        )
      pd  
    }
  }

  def cnvMultilingual(m: Map[Lang,String]): List[MonolingualTextValue] = 
    m.toList.map { 
      case (lang,text) => 
        new MonolingualTextValueImpl(text, lang.code)
    }

  def cnvItemId(id: ItemId): ItemIdValue = 
    new ItemIdValueImpl(id.id,id.iri.getLexicalForm)

  def cnvPropertyId(pd: PropertyId): PropertyIdValue = 
    new PropertyIdValueImpl(pd.id, pd.iri.getLexicalForm)

  def cnvStatements(ls: List[LocalStatement]): List[StatementGroup] = List()

  def cnvSiteLinks(sl: List[SiteLink]): List[WBSiteLink] = List()

  def cnvDatatype(dt: Datatype): DatatypeIdValue = new DatatypeIdImpl(dt.name)

}