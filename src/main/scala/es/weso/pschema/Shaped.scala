package es.weso.pschema

import es.weso.collection.Bag


case class Shaped[VD,L,E,P](
    value: VD, 
    shapesInfo: ShapesInfo[L,E] = ShapesInfo.default,
    outgoing: Option[Bag[(P,L)]] = None,
    waitFor:Map[VD, (P,L)] = Map[VD,(P,L)]()
  ) extends Serializable {

    def pendingShapes = shapesInfo.pendingShapes

    def addPendingShapes(shapes: Set[L]): Shaped[VD, L, E, P] =
      this.copy(shapesInfo = this.shapesInfo.addPendingShapes(shapes))
    def addOKShape(shape: L): Shaped[VD, L, E, P] = 
      this.copy(shapesInfo = this.shapesInfo.addOkShape(shape))
    def addNoShape(shape: L, err: E): Shaped[VD, L, E, P] = 
      this.copy(shapesInfo = this.shapesInfo.addNoShape(shape, err))

    def withOutgoing(bag: Bag[(P,L)]) = this.copy(outgoing = Some(bag))  
    def withoutPendingShapes = this.copy(shapesInfo = this.shapesInfo.withoutPendingShapes)

}
