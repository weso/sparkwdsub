package es.weso.pschema

import org.apache.spark.graphx._
import org.apache.spark.graphx.Pregel
import es.weso.collection._
import es.weso.collection.Bag._
import cats._
import cats.data._
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
  def apply[VD: ClassTag, ED: ClassTag, L: Ordering, E, P: Ordering](
    graph: Graph[VD,ED],
    initialLabel: L,
    maxIterations: Int = Int.MaxValue) 
    (checkLocal: (L, VD) => Either[E, Set[L]], 
     checkNeighs: (L, Bag[(P,L)]) => Either[E, Unit],
     getTripleConstraints: L => List[(P,L)], 
     cnvEdge: ED => P  
    ): Graph[Shaped[VD,L,E,P],ED] = {

  lazy val emptyBag: Bag[(P,L)] = Bag.empty[(P,L)] 

  def vprog(
    id: VertexId, 
    v: Shaped[VD, L, E, P], 
    msg: Msg[VD, L,E,P]
  ): Shaped[VD, L, E, P] = {

   // Update pending shapes looking at waiting for messages
   val v1 = v.pendingShapes.foldLeft(v) {
     case (v,pending) => {
       val waitingFor = msg.waitFor.collect { case (l,t) if l == pending => t } 
       v.withWaitingFor(pending,waitingFor,Set())
     }
   }

   val v2 = v1.waitingShapes.foldLeft(v1) {
     case (v,(waitingLabel,ws)) => {
       val notValidated = msg.notValidated.collect { case (l,t,e) if l == waitingLabel => (t,e) }
       if (notValidated.nonEmpty) {
         val es = NonEmptyList.fromList(notValidated.map(_._2).flatten.toList).get
         v.withFailedShape(waitingLabel, es)
       } else {
       val validated = msg.validated.collect { case (l,t) if l == waitingLabel => t }
       val rest = ws.ts.diff(validated)
       if (rest.isEmpty) {
         val neighsBag = mkBag(ws.validated.union(validated))
         val neighsChecked = 
          checkNeighs(waitingLabel, neighsBag) match {
           case Left(err) => v.addNoShape(waitingLabel,err)
           case Right(_) => v.addOkShape(waitingLabel)
         }
         neighsChecked
       } else {
         v.withWaitingFor(waitingLabel, rest, validated)
       }
     }
    }
   }

   // check requests to validate
   // Check requests to validate.
   // If they can validate locally withoug pending labels
   // they are directly added to OK shapes  
   // otherwise they are added to pending shapes
   val v3 = msg.validate.foldLeft(v2) {
    case (v,requestedLabel) => 
     checkLocal(requestedLabel,v.value) match {
      case Left(err) => v.addNoShape(requestedLabel,err)
      case Right(pendingLabels) => 
        if (v.unsolvedShapes contains requestedLabel) {
        // Recursion case when requested to validate a shape which is already pending
        // We are optimistic here and add it to okShapes
        // Other possibilities would be to include an Unknown status...
        v.addOkShape(requestedLabel)
      } else 
        if (pendingLabels.isEmpty) v.addOkShape(requestedLabel) 
        else v.addPendingShapes(pendingLabels)
    } 
   }
   println(s"""|VProg: vertexId: $id
               |Old value: ${v}
               |Msg received: ${msg}
               |NewValue: $v3
               |---------------------
               |""".stripMargin)  
   v3
  }

  def mkBag(s: Set[(VD,P,L)]): Bag[(P,L)] = 
    Bag.toBag(s.map { case (_,p,l) => (p,l) })

    def sendMsg(t: EdgeTriplet[Shaped[VD, L, E, P],ED]): Iterator[(VertexId, Msg[VD,L,E,P])] = {
      val shapeLabels = t.srcAttr.unsolvedShapes
      val ls = shapeLabels.map(sendMessagesPending(_, t)).toIterator.flatten
      ls
    }

    def sendMessagesPending(
      pendingLabel: L, 
      triplet: EdgeTriplet[Shaped[VD, L, E, P], ED]): Iterator[(VertexId, Msg[VD, L, E,P])] = {

     val tcs = getTripleConstraints(pendingLabel).filter(_._1 == cnvEdge(triplet.attr))
     println(s"""|sendMessagesPending(PendingLabel: ${pendingLabel}
                 |TripleConstraints: {${getTripleConstraints(pendingLabel).mkString(",")}}
                 |Triplet: ${triplet.srcAttr.value}-${cnvEdge(triplet.attr)}-${triplet.dstAttr.value})
                 |TCs: {${tcs.map(_._2.toString).mkString(",")}}""".stripMargin)
     tcs.toIterator.map{ case (p,l) => sendMessagesTripleConstraint(pendingLabel, p, l, triplet) }.flatten
    } 


    def sendMessagesTripleConstraint(
      pendingLabel: L,
      p: P, 
      label: L, 
      triplet: EdgeTriplet[Shaped[VD, L, E, P],ED]
      ): Iterator[(VertexId,Msg[VD,L,E,P])] = {
      val obj = triplet.dstAttr
      val msgs = if (obj.okShapes contains label) {
        // tell src that label has been validated
        List((triplet.srcId, validatedMsg(pendingLabel, p, label, triplet.dstAttr.value))) // Msg.validated(pendingLabel, p, label, triplet.dstAttr.value)))
      } else  if (obj.noShapes contains label) {
        val es = obj.failedShapes.map(_._2.toList.toSet).flatten
        List((triplet.srcId, notValidatedMsg(pendingLabel,p,label,triplet.dstAttr.value, es)))
      } else {
        // ask dst to validate label
        List(
          (triplet.dstId, validateMsg(label)),
          (triplet.srcId, waitForMsg(pendingLabel,p,label,triplet.dstAttr.value))
        )
      }
      println(s"Msgs: \n${msgs.mkString("\n")}")
      msgs.toIterator
    }

    def validateMsg(lbl: L): Msg[VD,L,E,P] = {
      Msg.validate[VD,L,E,P](Set(lbl))
    }

    def waitForMsg(lbl: L, p:P, l:L, v: VD): Msg[VD,L,E,P] = {
      Msg.waitFor[VD,L,E,P](lbl,p,l,v)
    }

    def notValidatedMsg(lbl: L, p: P, l: L, v: VD, es: Set[E]): Msg[VD,L,E,P] = {
      Msg.notValidated(lbl, p, l, v, es)
    }

    def validatedMsg(lbl: L, p: P, l: L, v: VD): Msg[VD,L,E,P] = {
      Msg.validated[VD,L,E,P](lbl, p, l, v)
    }
       
    def mergeMsg(p1: Msg[VD,L,E,P], p2: Msg[VD,L,E,P]): Msg[VD,L,E,P] = p1.merge(p2) 

    val shapedGraph: Graph[Shaped[VD, L, E, P], ED] = 
        graph.mapVertices{ case (vid,v) => Shaped[VD,L,E,P](v, Map()) } // , ShapesInfo.default) }

    val initialMsg: Msg[VD,L,E,P] = 
        Msg.validate(Set(initialLabel))

    // Invoke pregel algorithm from SparkX    
    Pregel(shapedGraph,initialMsg,maxIterations)(vprog,sendMsg, mergeMsg)
  }

}