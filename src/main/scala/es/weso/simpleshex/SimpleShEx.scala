package es.weso.simpleshex

import es.weso.wikibase._
import org.wikidata.wdtk.datamodel.implementation._
import org.wikidata.wdtk.datamodel.helpers.ItemDocumentBuilder
import org.wikidata.wdtk.datamodel.interfaces.EntityDocument
import org.wikidata.wdtk.datamodel.helpers.StatementBuilder
import org.wikidata.wdtk.datamodel.interfaces.Value;
import org.wikidata.wdtk.datamodel.interfaces._
import es.weso.rdf.nodes.IRI
import cats.data._ 
import cats.implicits._
import org.apache.spark.graphx.Edge
import es.weso.rbe.interval.IntOrUnbounded
import es.weso.rbe.interval.IntLimit
import es.weso.rbe.{Graph => _, _}
import es.weso.rbe.interval.IntervalChecker
import es.weso.collection.Bag
import es.weso.shex
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import es.weso.graphxhelpers.GraphBuilder._
import es.weso.graphxhelpers._


//



   