package es.weso.wdsub.spark.wbmodel

import es.weso.wdsub.spark.wbmodel.DumpUtils.{brackets, mkEntity, mkStatements}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.wikidata.wdtk.datamodel.helpers.JsonDeserializer

import java.nio.file.Path

case class LineParser(site: String = "http://www.wikidata.org/entity/") {

    lazy val jsonDeserializer = new JsonDeserializer(site)

    def line2Entity(line: String): (Long,Entity) = {
      val entityDocument = jsonDeserializer.deserializeEntityDocument(line)
      mkEntity(entityDocument)
    }

    def line2Statement(line: String): List[Edge[Statement]] = {
      val entityDocument = jsonDeserializer.deserializeEntityDocument(line)
      mkStatements(entityDocument)  
    }

    def lines2Entities(lines: List[String]): List[(Long,Entity)] = 
        lines.map(line2Entity(_)).toList

    def lines2Statements(lines:List[String]): List[Edge[Statement]] =
        lines.map(line2Statement(_)).toList.flatten

    def line2EntityStatements(line: String): (Long,Entity,List[Edge[Statement]]) = {
      val entityDocument = jsonDeserializer.deserializeEntityDocument(line)
      val (id, entity) = mkEntity(entityDocument)
      val ss = mkStatements(entityDocument)  
      (id,entity,ss)
    }

    def dumpPath2Graph(path: Path, sc: SparkContext): Graph[Entity,Statement] = {
      val fileName = path.toFile().getAbsolutePath()
      val fileLines: RDD[String] = sc.textFile(fileName)

      System.out.println("File lines -> " + fileLines.count())
     val all: RDD[(Long,Entity, List[Edge[Statement]])] =
      sc
      .textFile(fileName)
      .filter(!brackets(_))
      .map(line2EntityStatements(_))

     val vertices: RDD[(Long,Entity)] =
      all
      .map { case (id,v,_) => (id,v) }

     val edges: RDD[Edge[Statement]] =
      all
      .map { case (_,_, ss) => ss }
      .flatMap(identity)

     Graph(vertices,edges)
    }

    def dumpRDD2Graph(dumpLines: RDD[String], sc: SparkContext): Graph[Entity,Statement] = {
      val all: RDD[(Long,Entity, List[Edge[Statement]])] =
        dumpLines
          .filter(!brackets(_))
          .map(line2EntityStatements(_))

      val vertices: RDD[(Long,Entity)] =
        all
          .map { case (id,v,_) => (id,v) }

      val edges: RDD[Edge[Statement]] =
        all
          .map { case (_,_, ss) => ss }
          .flatMap(identity)

      Graph(
        vertices = vertices,
        edges = edges
      )
    }

    def dump2Graph(dump:String, sc: SparkContext): Graph[Entity,Statement] = {
        val lines = dump.split("\n").filter(!brackets(_)).toList
        val vertices = sc.parallelize(lines2Entities(lines))
        val edges = sc.parallelize(lines2Statements(lines))
        Graph(vertices,edges)
    }
}
