package es.weso.pschema

import org.apache.spark.graphx._
import org.apache.spark.graphx.Pregel
import es.weso.collection._
import es.weso.collection.Bag._
import cats._
import cats.implicits._
import scala.reflect.ClassTag

/**
 * Pregel Schema validation
 * 
 * Converts a Graph[VD,ED] into a Graph[ShapedValue[VD, L, E, P], ED]
 * 
 * where 
 * L = labels in Schema
 * E = type of errors
 * P = type of property identifiers
 **/ 
object PSchema {

/**
   * Execute a Pregel-like iterative vertex-parallel abstraction following 
   * a validationg schema.  
   *
   * On the first iteration all vertices receive the `initialMsg` and
   * on subsequent iterations if a vertex does not receive a message
   * then the vertex-program is not invoked.
   *
   * This function iterates until there are no remaining messages, or
   * for `maxIterations` iterations.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam L the type of labels in the schema
   * @tparam E the type of errors that happen when validating
   * @tparam P the type of properties (arcs in the graph)
   *
   * @param graph the input graph.
   *
   * @param initialLabel the start label
   *
   * @param maxIterations the maximum number of iterations to run for
   *
   * @param checkLocal the function that validates locally a 
   * vertex against a label in the schema
   * it returns either an error or if it validates, a set of pending labels
   * If there is no pending labels, the set will be empty
   *
   * @param checkNeighs it checks the bag of neighbours of a node against 
   * the regular bag expression defined by the label in the schema
   * 
   * @param getTripleConstraints returns the list of triple constraints
   *  associated with a label in a schema. A triple constraint is a pair with 
   * an arc and a pending label
   * 
   * @param cnvProperty a function that converts the type of edges to 
   * the type of arcs employed in the schema
   *
   * @return the resulting graph at the end of the computation with the
   * values embedded in a `ShapedValue` class that contains information 
   * about ok shapes, failed shapes,
   * inconsistent shapes and pending shapes.
   *
   */
   def apply[VD: ClassTag, ED: ClassTag, L, E, P: Ordering](
    graph: Graph[VD,ED],
    initialLabel: L,
    maxIterations: Int = Int.MaxValue) 
    (checkLocal: (L, VD) => Either[E, Set[L]], 
     checkNeighs: (L, Bag[P]) => Either[E, Unit],
     getTripleConstraints: L => List[(P,L)], 
     cnvProperty: ED => P  
    ): Graph[ShapedValue[VD,L,E,P],ED] = {

    lazy val emptyBag: Bag[P] = Bag.empty  

    def vprog(
      id: VertexId, 
      v: ShapedValue[VD, L, E, P], 
      msg: Msg[L,P]
      ): ShapedValue[VD, L, E, P] = {

       val pendingShapes = v.shapesInfo.pendingShapes 

       // Match outgoing bag of arcs with current pending shapes
       val checkedValue = 
        pendingShapes.foldLeft(v.withoutPendingShapes) {
         case (v, pending) => checkLocal(pending, v.value) match {
           case Left(err) => v.addNoShape(pending, err)
           case Right(ls) => {
             val neighsBag: Bag[P] = v.outgoing.getOrElse(msg.outgoing.getOrElse(emptyBag)) 
             // check neighs coming from msg
             val neighsChecked = checkNeighs(pending, neighsBag) match {
                 case Left(err) => v.addNoShape(pending,err)
                 case Right(_) => v.withOutgoing(neighsBag).addOKShape(pending)
               }
             neighsChecked
             }
           }
        }

      // Check requests to validate.
      // If they can validate locally withoug pending labels, 
      // they are not directly added to OK shapes  
      val newValue = msg.validate.foldLeft(checkedValue) {
        case (v,label) => checkLocal(label,v.value) match {
          case Left(err) => v.addNoShape(label,err)
          case Right(pending) => 
            if (pending.isEmpty) v.addOKShape(label) 
            else v.addPendingShapes(pending)
        } 
      }  

//       val newValue = checkedValue.addPendingShapes(msg.validate)
       println(s"VProg: vertexId: $id - newValue: $newValue")
       newValue
    }

    def sendMsg(t: EdgeTriplet[ShapedValue[VD, L, E, P],ED]): Iterator[(VertexId, Msg[L,P])] = {
      val shapeLabels = t.srcAttr.shapesInfo.pendingShapes
      val ls = shapeLabels.map(sendMessagesPending(_, t)).toIterator.flatten
      ls
    }

    def sendMessagesPending(
      shapeLabel: L, 
      triplet: EdgeTriplet[ShapedValue[VD,L,E,P],ED]): Iterator[(VertexId, Msg[L,P])] = {

     val tcs = getTripleConstraints(shapeLabel).filter(_._1 == cnvProperty(triplet.attr))
      println(s"sendMessagesPending(${triplet.srcAttr.value}-${cnvProperty(triplet.attr)}-${triplet.dstAttr.value}): ${tcs.map(_._2.toString).mkString(",")}")
      tcs.toIterator.map{ case (p,l) => sendMessagesTriplet(p, l, triplet) }.flatten
    } 

    def sendMessagesTriplet(p: P, label: L, triplet: EdgeTriplet[ShapedValue[VD, L, E, P],ED]): Iterator[(VertexId,Msg[L,P])] = {
      val msg1 = (triplet.srcId, Msg.outgoing[L,P](Bag(p))) // message to subject with outgoing arc
      val msg2 = (triplet.dstId, Msg.validate(Set(label)))  // message to object with pending shape
      println(s"Msg1: $msg1")
      println(s"Msg2: $msg2")
      Iterator(msg1,msg2)
    }

       
    def mergeMsg(p1: Msg[L,P], p2: Msg[L,P]): Msg[L,P] = p1.merge(p2) 

    val shapedGraph: Graph[ShapedValue[VD, L, E, P], ED] = 
        graph.mapVertices{ case (vid,v) => ShapedValue[VD,L,E,P](v, ShapesInfo.default) }

    val initialMsg: Msg[L,P] = 
        Msg[L,P](validate = Set(initialLabel))

    // Invoke pregel algorithm from SparkX    
    Pregel(shapedGraph,initialMsg,maxIterations)(vprog,sendMsg, mergeMsg)
  }

}