package es.weso.wbmodel

import org.wikidata.wdtk.datamodel.helpers.JsonDeserializer
import org.apache.spark.graphx._
import DumpUtils._
import org.apache.spark.SparkContext

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

    def dump2Graph(dump:String, sc: SparkContext): Graph[Entity,Statement] = {
        val lines = dump.split("\n").filter(!brackets(_)).toList
        val vertices = sc.parallelize(lines2Entities(lines))
        val edges = sc.parallelize(lines2Statements(lines))
        Graph(vertices,edges)
    }
}
