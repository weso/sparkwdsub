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

    def cnvProperty(p: Property): PropertyId = p.id

    val validatedGraph: Graph[ShapedValue[Value,ShapeLabel,Reason,PropertyId], Property] = 
      PSchema[Value,Property,ShapeLabel,Reason, PropertyId](
        graph, initialLabel, 5)(
          schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
        )

      // runShEx(SampleSchemas.schemaSimple, graph,2)
    println(s"Validated graph: ${validatedGraph.triplets.count()} triples")
    validatedGraph.vertices.collect().foreach(println(_))
    
    sc.stop()
 
  }


}