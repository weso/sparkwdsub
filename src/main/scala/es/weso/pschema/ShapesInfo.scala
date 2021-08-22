package es.weso.pschema

case class ShapesInfo[L,E](
    pendingShapes: Set[L] = Set(), 
    okShapes: Set[L] = Set(), 
    failedShapes: Set[(L,E)] = Set[(L,E)](),
    inconsistencies: Set[L] = Set[L]()
    ) {

    lazy val noShapes: Set[L] = failedShapes.map(_._1)    

    def replaceShapeBy(shape1: L, shape2: L) = 
      this.copy(pendingShapes = (this.pendingShapes - (shape1) + (shape2)))

    def addPendingShapes(shapes: Set[L]): ShapesInfo[L,E] =
      this.copy(pendingShapes = this.pendingShapes ++ shapes)

    def addOkShape(shape: L) = 
      if (inconsistencies.contains(shape))
       this.copy(pendingShapes = this.pendingShapes - shape)
      else if (noShapes.contains(shape)) 
       this.copy(pendingShapes = this.pendingShapes - shape, inconsistencies = this.inconsistencies + shape)
      else 
        this.copy(pendingShapes = this.pendingShapes - shape, okShapes = this.okShapes + shape)
    
    def addNoShape(shape: L, err: E) = 
      // TOOD: Do something with reason...
      if (inconsistencies.contains(shape)) 
        this.copy(pendingShapes = this.pendingShapes - shape)
      else if (okShapes.contains(shape)) 
        this.copy(pendingShapes = this.pendingShapes - shape, inconsistencies = this.inconsistencies + shape)
      else       
        this.copy(pendingShapes = this.pendingShapes - shape, failedShapes = this.failedShapes + ((shape,err)))

    def withoutPendingShapes(): ShapesInfo[L,E] = this.copy(pendingShapes = Set())    
  
    private def showPendingShapes(): String = s"Pending:${if (pendingShapes.isEmpty) "{}" else pendingShapes.mkString(",")}"
    private def showOKShapes(): String = if (okShapes.isEmpty) "" else okShapes.mkString(",")
    private def showNoShapes(): String = if (noShapes.isEmpty) "" else noShapes.map(_.toString).mkString(",")

    override def toString = showPendingShapes() + showOKShapes() + showNoShapes()
  
  }

  object ShapesInfo {
    def default[L,E]: ShapesInfo[L,E] = ShapesInfo(Set(),Set(),Set[(L,E)]())
  }
