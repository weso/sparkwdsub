package es.weso.sparkwdsub

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import es.weso.rdf.nodes._
import scala.jdk.CollectionConverters._
import es.weso.collection.Bag
import es.weso.graphxhelpers.GraphBuilder._
import es.weso.pschema._
import es.weso.simpleshex._
import org.apache.spark.sql.DataFrameStatFunctions

object SimpleApp {

  def main(args: Array[String]) {

    val master = "local"
    val partitions = 1

    lazy val spark: SparkSession = SparkSession
      .builder()
      .master(master)
      .appName("spark wdsub")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
  
    // val conf = new SparkConf().setAppName("Simple App").setMaster(master)
    // val sc = new SparkContext(conf)
    lazy val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    lazy val vs = spark
       .read
       .option("multiline",false)
       .json("examples/dump.json")

    vs.printSchema()   

    println(s"Columns: ${vs.columns.mkString(",")}")
    println(vs.col("id"))

    val vertices: RDD[(VertexId,Entity)] = sc.textFile("examples/example1.vertices").map(line => {
      val row = line.split(",")
      val id = row(0).toLong
      (id, Entity(row(1),id,row(2)))
    })

/*    val edges: RDD[(VertexId,Entity)] = sc.textFile("examples/example1.edges").map(line => {
      val row = line.split(",")
      val src = row(0).toLong
      val dst = row(1).toLong
//      val prop = Property(row(2),row(3),row)
      Edge(src, dst, prop)
    })*/

    println(vertices.collect().mkString("\n"))
//    val schema: Schema = SampleSchemas.schemaSimple

//    println(s"Graph triplets: ${graph.triplets.count()}")
//    graph.triplets.collect().foreach(println(_))

//    val initialLabel = ShapeLabel("Start")

//    def cnvProperty(p: Property): PropertyId = p.id

/*    val validatedGraph: Graph[ShapedValue[Value,ShapeLabel,Reason,PropertyId], Property] = 
      PSchema[Value,Property,ShapeLabel,Reason, PropertyId](
        graph, initialLabel, 5)(
          schema.checkLocal,schema.checkNeighs,schema.getTripleConstraints,_.id
        )

    println(s"Validated graph: ${validatedGraph.triplets.count()} triples")
    validatedGraph.vertices.collect().foreach(println(_))
*/    
    sc.stop()
 
  }


}