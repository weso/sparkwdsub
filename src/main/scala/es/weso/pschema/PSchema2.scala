package es.weso.pschema

import org.apache.spark.graphx._
import org.apache.spark.graphx.Pregel
import es.weso.collection._
import es.weso.collection.Bag._
import cats._
import cats.data._
import cats.implicits._
import scala.reflect.ClassTag
import org.apache.log4j.Logger

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
object PSchema2 extends Serializable {

  // transient annotation avoids log to be serialized
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def info(msg: String, verbose: Boolean) {
    if (verbose) {
      println(msg)
    }
    log.info(msg)
  }

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
   * The third parameter contains the triples that didn't validate
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
    maxIterations: Int = Int.MaxValue,
    verbose: Boolean = false
    ) 
    (checkLocal: (L, VD) => Either[E, Set[L]], 
     checkNeighs: (L, Bag[(P,L)], Set[(P,L)]) => Either[E, Unit],
     getTripleConstraints: L => List[(P,L)], 
     cnvEdge: ED => P  
    ): Graph[Shaped[VD,L,E,P],ED] = {

  lazy val emptyBag: Bag[(P,L)] = Bag.empty[(P,L)] 

  def vprog(
    id: VertexId, 
    v: Shaped[VD, L, E, P], 
    msg: MsgMap[L, E, P]
  ): Shaped[VD, L, E, P] = {
    msg.mmap.foldLeft(v){
     case (acc,(lbl,msgLabel)) => vProgLabel(acc,lbl,msgLabel)
    }
  }

  def vProgLabel(
    acc: Shaped[VD, L, E, P], 
    lbl: L,
    msgLabel: MsgLabel[L, E, P]): Shaped[VD, L, E, P] = {
     msgLabel match {
      case ValidateLabel => acc.statusMap.get(lbl) match {
        case None => acc.addPendingShapes(Set(lbl))
        case Some(Pending) => checkLocal(lbl, acc.value) match {
          case Left(e) => acc.addNoShape(lbl,NonEmptyList.one(e))
          case Right(lbls) => acc.addPendingShapes(lbls)
        }
        case Some(WaitingFor(ds,oks,failed)) => acc
        case Some(Ok) => acc
        case Some(Failed(_)) => acc
        case Some(Inconsistent) => acc
      }
      case vd: Validated[L,P] => acc.statusMap.get(lbl) match {
        case None => acc.addOkShape(lbl)
        case Some(Pending) => acc.addOkShape(lbl)
        case Some(wf : WaitingFor[_, L,E, P]) => { 
          val rest = wf.dependants.diff(vd.dt)
          if (rest.isEmpty) {
            val bag = mkBag(wf.validated ++ vd.dt)
            val failed = mkFailed(wf.notValidated)
            checkNeighs(lbl, bag, failed) match {
              case Left(e) => acc.addNoShape(lbl, NonEmptyList.one(e))
              case Right(()) => acc.addOkShape(lbl)
            }
          } else {
            acc.withWaitingFor(lbl, rest, wf.validated ++ vd.dt, wf.notValidated)
          }
        }
        case Some(Ok) => acc
        case Some(Failed(_)) => acc.addInconsistent(lbl)
        case Some(Inconsistent) => acc.addInconsistent(lbl)
      }
      case nvd: NotValidated[L,E,P] => acc.statusMap.get(lbl) match {
        case None => {
          val es = flattenNels(nvd.dtes.map(_._2))
          acc.addNoShape(lbl,es) 
        }
        case Some(Pending) => {
          val es = flattenNels(nvd.dtes.map(_._2))
          acc.addNoShape(lbl, es)
        }
        case Some(wf: WaitingFor[_,L,E,P]) => {
          val dt = nvd.dtes.map(_._1)
          val rest = wf.dependants.diff(dt)
          if (rest.isEmpty) {
            val bag = mkBag(wf.validated)
            val failed: Set[(P,L)] = mkFailed(wf.notValidated)
            val newFailed = nvd.dtes.map{ case (dt,es) => (dt.prop, dt.label) }
            checkNeighs(lbl, bag, failed ++ newFailed) match {
              case Left(e) => acc.addNoShape(lbl, NonEmptyList.one(e))
              case Right(()) => acc.addOkShape(lbl)
            }
          } else {
            // TODO!!
            acc.withWaitingFor(lbl, rest, wf.validated, wf.notValidated)
          }
        }
        case Some(Ok) => acc.addInconsistent(lbl)
        case Some(Failed(_)) => acc
        case Some(Inconsistent) => acc.addInconsistent(lbl)        
      }
      case inc: InconsistentLabel[L,E,P] => acc.statusMap.get(lbl) match {
        case None => acc.addInconsistent(lbl)
        case Some(_) => acc.addInconsistent(lbl)
      }
      case wf: WaitFor[L,P] => acc.statusMap.get(lbl) match {
        case None => acc.withWaitingFor(lbl, wf.ds, Set(), Set())
        case Some(Pending) => acc.withWaitingFor(lbl, wf.ds, Set(), Set())
        case Some(wf1: WaitingFor[_,L,E,P]) => {
          val ds = wf.ds union wf1.dependants
          acc.withWaitingFor(lbl, ds, wf1.validated, wf1.notValidated)
        }
        case Some(Ok) => acc
        case Some(Failed(_)) => acc
        case Some(Inconsistent) => acc
      }
    }
  }

  def flattenNels[E](ess: Set[NonEmptyList[E]]): NonEmptyList[E] = { 
    val xs: List[E] = ess.map(_.toList).toList.flatten
    NonEmptyList.fromListUnsafe(xs)
  }

  def mkBag(s: Set[DependTriple[P,L]]): Bag[(P,L)] = 
    Bag.toBag(s.map { case t => (t.prop,t.label) })

  def mkFailed(s: Set[(DependTriple[P,L], Set[E])]): Set[(P,L)] = 
    s.map { case (t,_) => (t.prop,t.label) }

  def sendMsg(t: EdgeTriplet[Shaped[VD, L, E, P],ED]): Iterator[(VertexId, MsgMap[L, E, P])] = {
      val shapeLabels = t.srcAttr.unsolvedShapes // active shapes = waitingFor/pending
      val ls = shapeLabels.map(sendMessagesActive(_, t)).toIterator.flatten
      ls
  }

  def sendMessagesActive(
      activeLabel: L, 
      triplet: EdgeTriplet[Shaped[VD, L, E, P], ED]): Iterator[(VertexId, MsgMap[L, E, P])] = {

     val tcs = getTripleConstraints(activeLabel).filter(_._1 == cnvEdge(triplet.attr))
     info(s"""|sendMessagesActive(label: ${activeLabel}
                 |TripleConstraints: {${getTripleConstraints(activeLabel).mkString(",")}}
                 |Triplet: ${triplet.srcAttr.value}-${cnvEdge(triplet.attr)}-${triplet.dstAttr.value})
                 |TCs: {${tcs.map(_._2.toString).mkString(",")}}""".stripMargin, verbose)
     tcs.toIterator.map{ 
       case (p,l) => 
        sendMessagesTripleConstraint(activeLabel, p, l, triplet) 
      }.flatten
  } 


  def sendMessagesTripleConstraint(
      pendingLabel: L,
      p: P, 
      label: L, 
      triplet: EdgeTriplet[Shaped[VD, L, E, P],ED]
      ): Iterator[(VertexId,MsgMap[L, E, P])] = {
      val obj = triplet.dstAttr
      val msgs = if (obj.okShapes contains label) {
        // tell src that label has been validated
        List(
          (triplet.srcId, validatedMsg(pendingLabel, p, label, triplet.dstId))
        ) 
      } else  if (obj.noShapes contains label) {
        val es = NonEmptyList.fromListUnsafe(obj.failedShapes.map(_._2.toList).flatten.toList)
        List(
          (triplet.srcId, notValidatedMsg(pendingLabel,p,label,triplet.dstId, es))
        )
      } else {
        // ask dst to validate label
        List(
          (triplet.dstId, validateMsg(label)),
          (triplet.srcId, waitForMsg(pendingLabel,p,label,triplet.dstId))
        )
      }
      info(s"""|Msgs for triplet (${triplet.srcId}-${triplet.attr}->${triplet.dstId}): 
                   |${msgs.mkString("\n")}
                   |""".stripMargin, verbose)
      msgs.toIterator
  }

  def validateMsg(lbl: L): MsgMap[L, E, P] = {
      MsgMap.validate[L,E,P](Set(lbl))
  }

  def waitForMsg(lbl: L, p:P, l:L, v: VertexId): MsgMap[L, E, P] = {
      MsgMap.waitFor[L,E,P](lbl,p,l,v)
  }

  def notValidatedMsg(lbl: L, p: P, l: L, v: VertexId, es: NonEmptyList[E]): MsgMap[L,E,P] = {
      MsgMap.notValidated(lbl, p, l, v, es)
  }

  def validatedMsg(lbl: L, p: P, l: L, v: VertexId): MsgMap[L,E,P] = {
      MsgMap.validated[L,E,P](lbl, p, l, v)
  }
       
  def mergeMsg(p1: MsgMap[L,E,P], p2: MsgMap[L,E,P]): MsgMap[L,E,P] = p1.merge(p2) 

  val shapedGraph: Graph[Shaped[VD, L, E, P], ED] = 
        graph.mapVertices{ case (vid,v) => Shaped[VD,L,E,P](v, Map()) } // , ShapesInfo.default) }

  val initialMsg: MsgMap[L,E,P] = 
        MsgMap.validate(Set(initialLabel))

  // Invoke pregel algorithm from SparkX    
  Pregel(shapedGraph,initialMsg,maxIterations)(vprog, sendMsg, mergeMsg)
  }

}