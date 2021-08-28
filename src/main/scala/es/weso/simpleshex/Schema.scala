package es.weso.simpleshex

import es.weso.collection.Bag
import es.weso.wbmodel._
import es.weso.rdf.PrefixMap
import cats._ 
import cats.implicits._
import java.nio.file.Path
import cats.effect.IO


sealed trait WShExFormat
case object CompactFormat extends WShExFormat 
case object JSONFormat extends WShExFormat

sealed abstract class ParseError(msg: String) extends Product with Serializable
case class ParseException(e: Throwable) extends ParseError(e.getMessage())
case class ConversionError(e: ConvertError) extends ParseError(s"Error converting shEx to WShEx\nError: ${e}")

case class Schema(
  shapesMap: Map[ShapeLabel, ShapeExpr],
  start: Option[ShapeExpr] = None,
  pm: PrefixMap = PrefixMap.empty
  ) extends Serializable {

 def get(shapeLabel: ShapeLabel): Option[ShapeExpr] = shapeLabel match {
   case Start => start 
   case _ => shapesMap.get(shapeLabel)
 }

 def checkLocal(label: ShapeLabel, entity: Entity): Either[Reason, Set[ShapeLabel]] = {
   get(label) match {
     case None => Left(ShapeNotFound(label,this))
     case Some(se) => se.checkLocal(entity, label)
   }
 }

 def checkNeighs(label: ShapeLabel, neighs: Bag[(PropertyId,ShapeLabel)]): Either[Reason, Unit] = {
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

object Schema {

 private def cnvFormat(format: WShExFormat): String = format match {
        case CompactFormat => "ShEXC"
        case JSONFormat => "JSON"
    }

    
 def unsafeFromString(str: String, format: WShExFormat): Either[ParseError, Schema] = {
        import cats.effect.unsafe.implicits.global
        try {
          val schema = es.weso.shex.Schema.fromString(str,cnvFormat(format)).unsafeRunSync()
          val wShEx = ShEx2SimpleShEx.convertSchema(schema)
          wShEx.bimap(ConversionError(_), identity)
        } catch {
            case e: Exception => ParseException(e).asLeft
        }
    }

 def fromPath(
   path: Path, 
   format: WShExFormat = CompactFormat
   ): IO[Schema] = for {
    schema <- es.weso.shex.Schema.fromFile(path.toFile().getAbsolutePath(), cnvFormat(format))
    resolvedSchema <- es.weso.shex.ResolvedSchema.resolve(schema, None)
    schema <- IO.fromEither(ShEx2SimpleShEx.convertSchema(resolvedSchema))
  } yield schema

 def unsafeFromPath(
   path: Path, 
   format: WShExFormat = CompactFormat
   ): Schema = {
    import cats.effect.unsafe.implicits.global
    fromPath(path, format).unsafeRunSync()
  }
}
