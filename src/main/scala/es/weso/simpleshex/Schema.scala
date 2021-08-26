package es.weso.simpleshex

import es.weso.collection.Bag

case class Schema(map: Map[ShapeLabel, ShapeExpr]) extends Serializable {

    def get(shapeLabel: ShapeLabel): Option[ShapeExpr] = 
      map.get(shapeLabel)

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
