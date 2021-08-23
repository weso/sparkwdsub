package es.weso.pschema

import es.weso.collection.Bag


case class ShapedValue[VD,L,E,P](
    value: VD, 
    shapesInfo: ShapesInfo[L,E] = ShapesInfo.default,
    outgoing: Option[Bag[P]] = None
  ) extends Serializable {

    def pendingShapes = shapesInfo.pendingShapes

    def addPendingShapes(shapes: Set[L]): ShapedValue[VD, L, E, P] =
      this.copy(shapesInfo = this.shapesInfo.addPendingShapes(shapes))
    def addOKShape(shape: L): ShapedValue[VD, L, E, P] = 
      this.copy(shapesInfo = this.shapesInfo.addOkShape(shape))
    def addNoShape(shape: L, err: E): ShapedValue[VD, L, E, P] = 
      this.copy(shapesInfo = this.shapesInfo.addNoShape(shape, err))

    def withOutgoing(bag: Bag[P]) = this.copy(outgoing = Some(bag))  
    def withoutPendingShapes = this.copy(shapesInfo = this.shapesInfo.withoutPendingShapes)

}
