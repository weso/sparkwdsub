package es.weso.pschema

import es.weso.collection.Bag


case class ShapedValue[VD,L,E,P](
    value: VD, 
    shapesInfo: ShapesInfo[L,E] = ShapesInfo.default,
    outgoing: Option[Bag[P]] = None
  ) extends Serializable {

    def addPendingShapes(shapes: Set[L]): ShapedValue[VD, L, E, P] =
      this.copy(shapesInfo = this.shapesInfo.addPendingShapes(shapes))
    def addOKShape(shape: L): ShapedValue[VD, L, E, P] = 
      this.copy(shapesInfo = this.shapesInfo.addOkShape(shape))
    def addNoShape(shape: L, err: E): ShapedValue[VD, L, E, P] = 
      this.copy(shapesInfo = this.shapesInfo.addNoShape(shape, err))

    def withOutgoing(bag: Bag[P]) = this.copy(outgoing = Some(bag))  
    def withoutPendingShapes = this.copy(shapesInfo = this.shapesInfo.withoutPendingShapes)

/*    def validatePendingShapes(schema: Schema, shapes: Set[ShapeLabel]): ShapedValue = 
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
*/
  }
