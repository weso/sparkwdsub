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
    
    private def showSet(name: String, s: Set[L]): String =
      s"$name:{${s.map(_.toString).mkString(",")}}"
  
    private def showPendingShapes(): String = 
      showSet("Pending", pendingShapes)
    private def showOKShapes(): String = 
      showSet(", OK", okShapes)
    private def showNoShapes(): String = 
      showSet(", NO", noShapes)

    override def toString = s"${showPendingShapes()}, ${showOKShapes()}, ${showNoShapes()}"
  

    def visited(l: L): Boolean = {
      inconsistencies.contains(l) ||
      okShapes.contains(l) ||
      noShapes.contains(l) 
    }

  }

  object ShapesInfo {
    def default[L,E]: ShapesInfo[L,E] = ShapesInfo(Set(),Set(),Set[(L,E)]())
  }
