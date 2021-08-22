package es.weso.sparkwdsub

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import Helpers._
import es.weso.rdf.nodes._
import scala.jdk.CollectionConverters._
import es.weso.collection.Bag
import es.weso.pschema.GraphBuilder._
import es.weso.pschema._

object SimpleApp {

  /* def runShEx(schema: Schema, graph: Graph[Value, Property], maxIterations: Int = Int.MaxValue): Graph[ShapedValue,Property] = {

    val shapedGraph: Graph[ShapedValue,Property] = 
      graph.mapVertices{ case (vid,value) => ShapedValue(value)}

    // We start with pending shape Start
    def initialMsg: Msg = Msg(validate = Set(ShapeLabel("Start")))

    def vprog(schema: Schema)(
      id: VertexId, 
      v: ShapedValue, 
      msg: Msg): ShapedValue = {
       println(s"vprog: $id, v: $v, msg: $msg") 
       // Match outgoing bag of arcs with current pending shapes
       val checkedValue = checkBag(v, Bag.toBag(msg.outgoing), schema)
       // Add pending shapes from msg
       val newValue = v.validatePendingShapes(schema, msg.validate)
//       val newValue = checkedValue.addPendingShapes(msg.validate) 
       println(s"VProg: vertexId: $id - newValue: $newValue")
       newValue
    }


    def checkBag(
      v: ShapedValue, 
      neighs: Bag[PropertyId], 
      schema: Schema
      ): ShapedValue = {

      val newValue = v.shapesInfo.pendingShapes.foldLeft(v){ 
        case (v,shapeLabel) => schema.get(shapeLabel) match {
          case None => v.addNoShape(shapeLabel, ShapeNotFound(shapeLabel, schema))
          case Some(se) => se.checkNeighs(neighs) match {
            case Left(reason) => v.addNoShape(shapeLabel,reason)
            case Right(_) => v.addOKShape(shapeLabel)
          }
        }
      }
    
      println(s"After checkBag: $v ~ $neighs\n$newValue")
      newValue
    }

    def sendMsg(schema: Schema)(t: EdgeTriplet[ShapedValue,Property]): Iterator[(VertexId, Msg)] = {
      val shapeLabels = t.srcAttr.shapesInfo.pendingShapes
      val ls = shapeLabels.map(sendMessagesPending(_, t, schema)).toIterator.flatten
      ls
    }

    def sendMessagesPending(
      shapeLabel: ShapeLabel, 
      triplet: EdgeTriplet[ShapedValue,Property], 
      schema: Schema): Iterator[(VertexId, Msg)] = {
      val tcs = schema.getTripleConstraints(shapeLabel).filter(_.property == triplet.attr.id)
      println(s"sendMessagesPending(${triplet.srcAttr.value}-${triplet.attr.id}-${triplet.dstAttr.value}): ${tcs.map(_.toString).mkString(",")}")
      tcs.toIterator.map(sendMessagesTriplet(_, triplet)).flatten
    } 

    def sendMessagesTriplet(tc: TripleConstraint, triplet: EdgeTriplet[ShapedValue,Property]): Iterator[(VertexId,Msg)] = {
      val msg1 = (triplet.srcId, Msg.outgoing(Set(triplet.attr.id))) // message to subject with outgoing arc
      val msg2 = (triplet.dstId, Msg.validate(Set(tc.value.label)))  // message to object with pending shape
      println(s"Msg1: $msg1")
      println(s"Msg2: $msg2")
      Iterator(msg1,msg2)
    }

    def mergeMsg(p1: Msg, p2: Msg): Msg = p1.merge(p2) 

    val validated: Graph[ShapedValue,Property] = 
      shapedGraph.pregel(initialMsg, maxIterations)(
        vprog(schema), 
        sendMsg(schema), 
        mergeMsg)

    validated    
  } */

  def main(args: Array[String]) {

    val master = "local"
    val conf = new SparkConf().setAppName("Simple App").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val graph = buildGraph(SampleSchemas.simpleGraph1,sc)
    val schema: Schema = SampleSchemas.schemaSimple

    println(s"Graph triplets: ${graph.triplets.count()}")
    graph.triplets.collect().foreach(println(_))

    val initialLabel = ShapeLabel("Start")
    def checkLocal(schema:Schema)(label: ShapeLabel, value: Value): Either[Reason, Set[ShapeLabel]] = {
      schema.get(label) match {
        case None => Left(ShapeNotFound(label,schema))
        case Some(se) => se.checkLocal(value)
      }
    }

    def checkNeighs(schema: Schema)(label: ShapeLabel, neighs: Bag[PropertyId]): Either[Reason, Unit] = {
      schema.get(label) match {
        case None => Left(ShapeNotFound(label,schema))
        case Some(se) => se.checkNeighs(neighs)
      }
    }

    def getTripleConstraints(schema: Schema)(label: ShapeLabel): List[(PropertyId, ShapeLabel)] = {
      schema.get(label) match {
        case None => List()
        case Some(se) => se.tripleConstraints.map(tc => (tc.property, tc.value.label))
      }
    }

    def cnvProperty(p: Property): PropertyId = p.id

    val validatedGraph: Graph[ShapedValue[Value,ShapeLabel,Reason,PropertyId], Property] = 
      PSchema[Value,Property,ShapeLabel,Reason, PropertyId](graph, initialLabel, 5)(checkLocal(schema),checkNeighs(schema),getTripleConstraints(schema),cnvProperty)

      // runShEx(SampleSchemas.schemaSimple, graph,2)
    println(s"Validated graph: ${validatedGraph.triplets.count()} triples")
    validatedGraph.vertices.collect().foreach(println(_))
    
    sc.stop()
 
  }


}