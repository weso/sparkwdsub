package es.weso.simpleshex

import es.weso.collection.Bag

case class Schema(map: Map[ShapeLabel, ShapeExpr]) extends Serializable {

    def get(shapeLabel: ShapeLabel): Option[ShapeExpr] = 
      map.get(shapeLabel)

    def checkLocal(label: ShapeLabel, value: Value): Either[Reason, Set[ShapeLabel]] = {
      get(label) match {
        case None => Left(ShapeNotFound(label,this))
        case Some(se) => se.checkLocal(value, label)
      }
    }

    def checkNeighs(label: ShapeLabel, neighs: Bag[Value.PropertyId]): Either[Reason, Unit] = {
      get(label) match {
        case None => Left(ShapeNotFound(label,this))
        case Some(se) => se.checkNeighs(neighs)
      }
    }

    def getTripleConstraints(label: ShapeLabel): List[(Value.PropertyId, ShapeLabel)] = {
      get(label) match {
        case None => List()
        case Some(se) => se.tripleConstraints.map(tc => (tc.property, tc.value.label))
      }
    }


  }
