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

  def apply[VD: ClassTag, ED: ClassTag, L, E, P: Ordering](
    graph: Graph[VD,ED],
    initialLabel: L,
    maxIterations: Int = Int.MaxValue) 
    (checkLocal: (L, VD) => Either[E, Set[L]], 
     checkNeighs: (L, Bag[P]) => Either[E, Unit],
     getTripleConstraints: L => List[(P,L)], 
     cnvProperty: ED => P  
    ): Graph[ShapedValue[VD,L,E,P],ED] = {

    def vprog(id: VertexId, v: ShapedValue[VD, L, E, P], msg: Msg[L,P]): ShapedValue[VD, L, E, P] = {

       val pendingShapes = v.shapesInfo.pendingShapes 
       var newPending = Set[L]()

       // Match outgoing bag of arcs with current pending shapes
       val checkedValue = pendingShapes.foldLeft(v.withoutPendingShapes) {
         case (v, pending) => checkLocal(pending, v.value) match {
           case Left(err) => v.addNoShape(pending, err)
           case Right(ls) => {
             val locallyChecked = v.addPendingShapes(Set(pending))

             val neighsBag: Option[Bag[P]] = v.outgoing orElse msg.outgoing 

             // check neighs coming from msg
             val neighsChecked = neighsBag match {
               case None => locallyChecked
               case Some(bag) => checkNeighs(pending, bag) match {
                 case Left(err) => locallyChecked.addNoShape(pending,err)
                 case Right(_) => locallyChecked.withOutgoing(bag).addOKShape(pending)
               }
             }
             neighsChecked
           }
         }
       }

       val newValue = checkedValue.addPendingShapes(msg.validate)
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