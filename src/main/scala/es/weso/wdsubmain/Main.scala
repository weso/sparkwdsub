package es.weso.sparkwdsub

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
// import es.weso.wshex._
import Helpers._
import es.weso.rdf.nodes._
import scala.jdk.CollectionConverters._

object SimpleApp {

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

    val shapedGraph: Graph[ShapedValue,Property] = graph.mapVertices{ case (vid,value) => ShapedValue(value)}

   
    def maxIterations = 3
    
    // The messages indicate the pending shapes on a node
    type Msg = Set[ShapeLabel]

    // We start with pending shape Start
    def initialMsg: Msg = Set(ShapeLabel("Start"))

    def vprog(id: VertexId, v: ShapedValue, msg: Msg): ShapedValue = {
      val newValue = checkShapes(v, msg) 
      println(s"VProg: vertexId: $id - newValue: $newValue")
      newValue
    }

    def checkShapes(v: ShapedValue, shapes: PendingShapes): ShapedValue = {
      shapes.foldLeft(v)(checkShape)
    }

    def checkShape(v: ShapedValue, shape: ShapeLabel): ShapedValue = shape.name match {
      case "Start" => v.replaceShapeBy(shape, ShapeLabel("Researcher"))
      case "Human" => v.value match {
        case e: Entity if e.id == "Q5" => v.addOKShape(shape)
        case _ => v.addNoShape(shape, ValueIsNot("Q5"))   
      }
//      case "Human" => e.validated(shape)
//      case "Researcher" => e.validated(shape)
      case _ => v.addOKShape(shape)
    }

    def sendMsg(t: EdgeTriplet[ShapedValue,Property]): Iterator[(VertexId, Msg)] = {
//      println(s"sendMsg: Source vertex: ${t.srcAttr}\npendingShapes: ${t.srcAttr.shapesInfo.pendingShapes}")
//      println(s"sendMsg: Property ${t.attr}")

      val xs = t.srcAttr.shapesInfo.pendingShapes.toList.map(sendMsgPendingShape(_,t))
      val vs: List[(VertexId,Msg)] = xs.flatten
      vs.toIterator
    }

    lazy val noMore: List[(VertexId, PendingShapes)] = List()

    def sendMsgPendingShape(shapeLabel: ShapeLabel, t: EdgeTriplet[ShapedValue,Property]): List[(VertexId, PendingShapes)] =  
      shapeLabel.name match {
        case "Start" => List((t.srcId, Set(ShapeLabel("Researcher"))))
        case "Researcher" => t.attr.id match {
          case "P31" => List((t.dstId, Set(ShapeLabel("Human"))))
          case "P19" => List((t.dstId, Set(ShapeLabel("Place"))))
          case _ => noMore
        }
        case "Human" => noMore
        case "Place" => noMore
        case _ => noMore
      }

    def mergeMsg(p1: Msg, p2: Msg): Msg = p1.union(p2) 

    val validated = shapedGraph.pregel(initialMsg, maxIterations)(vprog,sendMsg, mergeMsg)
    println(s"Validated triplets: ${validated.triplets.count()}")
    validated.triplets.collect().foreach(println(_))


/*    val updated = graph.mapVertices{ case (vid, value) => value.addShapeLabel(ShapeLabel("Map")) }
    println(s"Updated triplets: ${updated.triplets.count()}")
    updated.triplets.collect().foreach(println(_)) */

    sc.stop()
 
  }

  def validateShapes(shapes: Set[ShapeLabel], value: Value): Value = shapes.foldRight(value){ case (shape, v) => validateShape(shape, v)}
  
  def validateShape(shape: ShapeLabel, value: Value): Value = shape.name match {
    case "Start" => value match {
     case e: Entity => 
      Entity(id = e.id, vertexId = e.vertexId, label = e.label, shapesInfo = ShapesInfo(pendingShapes = e.shapesInfo.pendingShapes, okShapes = e.shapesInfo.okShapes.union(Set(shape)), noShapes = e.shapesInfo.noShapes))
     case _ => copy(value)
    }
    case "Researcher" => value match {
      case e: Entity => value
      case _ => value
    }
  }

  def copy(value: Value): Value = value


    // value // value.addPendingShapes(shapes)

}