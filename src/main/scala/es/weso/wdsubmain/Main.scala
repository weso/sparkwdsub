package es.weso.sparkwdsub

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import Helpers._
import es.weso.rdf.nodes._
import scala.jdk.CollectionConverters._
import es.weso.collection.Bag

object SimpleApp {

  def runShEx(schema: Schema, graph: Graph[Value, Property]): Graph[ShapedValue,Property] = {

    val shapedGraph: Graph[ShapedValue,Property] = 
      graph.mapVertices{ case (vid,value) => ShapedValue(value)}

    def maxIterations = 3

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
      val tcs = schema.getTripleConstraints(shapeLabel).filter(_.property == triplet.attr.id).toIterator
      println(s"sendMessagesPending: $tcs")
      tcs.map(sendMessagesTriplet(_, triplet)).flatten
    } 

    def sendMessagesTriplet(tc: TripleConstraint, triplet: EdgeTriplet[ShapedValue,Property]): Iterator[(VertexId,Msg)] = {
      Iterator(
       (triplet.srcId, Msg.outgoing(Set(triplet.attr.id))), // message to subject with outgoing arc
       (triplet.dstId, Msg.validate(Set(tc.value.label))) // message to object with pending shape
      )
    }

    def mergeMsg(p1: Msg, p2: Msg): Msg = p1.merge(p2) 

    val validated: Graph[ShapedValue,Property] = 
      shapedGraph.pregel(initialMsg, maxIterations)(
        vprog(schema), 
        sendMsg(schema), 
        mergeMsg)

    validated    
  }

  def main(args: Array[String]) {

    val master = "local"
    val conf = new SparkConf().setAppName("Simple App").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val builder: Builder[(Seq[Value], Seq[Edge[Property]])] = for {
      human <- Q(5, "Human")
      dAdams <- Q(42, "Douglas Adams")
      timBl <- Q(80, "Tim Beners-Lee")
      instanceOf <- P(31, "instanceOf")
      cern <- Q(42944, "CERN")
      uk <- Q(145, "UK")
      paAward <- Q(3320352, "Princess of Asturias Award") 
      spain <- Q(29, "Spain")
      country <- P(17,"country")
      employer <- P(108,"employer")
      birthPlace <- P(19, "place of birth")
      london <- Q(84, "London")
      awardReceived <- P(166, "award received")
      togetherWith <- P(1706, "together with")
      y1980 <- Date("1980")
      y1984 <- Date("1984")
      y1994 <- Date("1994")
      y2002 <- Date("2002")
      y2013 <- Date("2013")
      vintCerf <- Q(92743, "Vinton Cerf")
      start <- P(580, "start time")
      end <- P(582, "end time")
      time <- P(585, "point in time")
    } yield {
      vertexEdges(
        (timBl, instanceOf, human, List()),
        (timBl, birthPlace, london, List()),
        (timBl, employer, cern, List(Qualifier(start, y1980), Qualifier(end, y1980))),
        (timBl, employer, cern, List(Qualifier(start, y1984), Qualifier(end, y1994))),
        (timBl, awardReceived, paAward, List(Qualifier(togetherWith, vintCerf), Qualifier(time, y2002))),
        (dAdams, instanceOf, human, List()), 
        (paAward, country, spain, List()),
        (vintCerf,instanceOf, human, List()),
        (vintCerf, awardReceived, paAward, List(Qualifier(togetherWith, timBl), Qualifier(time, y2002))),
        (cern, awardReceived, paAward, List(Qualifier(time,y2013)))
      )
    }

    val (es,ss) = build(builder)

    val entities: RDD[(VertexId, Value)] = 
      sc.parallelize(es.map(e => (e.vertexId, e)))
    
    // Create an RDD for edges
    val edges: RDD[Edge[Property]] =
      sc.parallelize(ss)

    val graph = Graph(entities, edges)    
    println(s"Graph triplets: ${graph.triplets.count()}")
    graph.triplets.collect().foreach(println(_))


    val schema = schemaResearcher

    val validatedGraph = runShEx(schema, graph)
    println(s"Validated triplets: ${validatedGraph.triplets.count()}")
    validatedGraph.triplets.collect().foreach(println(_))
    
    sc.stop()
 
  }


}