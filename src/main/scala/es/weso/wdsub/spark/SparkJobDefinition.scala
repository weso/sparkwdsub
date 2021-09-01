package es.weso.wdsub.spark

import es.weso.wdsub.spark.wbmodel.LineParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Calendar

class SparkJobDefinition(sparkJobConfig: SparkJobConfig) {

  // Create the result file for later use.
  val resultFile = new ResultFile()
  val date = Calendar.getInstance().getTime();
  val dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
  val strDate = dateFormat.format(date);
  resultFile.jobName = sparkJobConfig.jobName.apply()
  resultFile.jobDate = strDate

  // Create the spark context that will be initialized depending on the job mode.
  var sparkContext: SparkContext = null
  if( SparkJobDefinitionMode.fromString(sparkJobConfig.jobMode.apply()).equals(SparkJobDefinitionMode.Test) ) {
    sparkContext = SparkSession.builder().master("local[*]").appName(sparkJobConfig.jobName.apply()).getOrCreate().sparkContext
  } else if( SparkJobDefinitionMode.fromString(sparkJobConfig.jobMode.apply()).equals(SparkJobDefinitionMode.Cluster) ) {
    sparkContext = new SparkContext( new SparkConf().setAppName( sparkJobConfig.jobName.apply()) )
  }

  // Start measuring the execution time.
  val jobStartTime = System.nanoTime

  // Load the dump in to an RDD of type String. The RDD will be composed of each single line as a String.
  val dumpLines: RDD[String] = sparkContext.textFile( sparkJobConfig.jobInputDump.apply() )
  resultFile.jobResults = ""//"Dump Lines -> " + dumpLines.count().toString

  val lineParser = LineParser()
  val graph = lineParser.dumpRDD2Graph(dumpLines, sparkContext)

  resultFile.jobResults += "\n Graph Edges: " + graph.edges.count()
  resultFile.jobResults += "\n Graph Vertices: " + graph.vertices.count()

  // Get the job execution time in seconds.
  val jobExecutionTime = (System.nanoTime - jobStartTime) / 1e9d
  val jobCores = java.lang.Runtime.getRuntime.availableProcessors * ( sparkContext.statusTracker.getExecutorInfos.length -1 )
  val jobMem = java.lang.Runtime.getRuntime.totalMemory() * ( sparkContext.statusTracker.getExecutorInfos.length -1 )

  resultFile.time = jobExecutionTime.toString
  resultFile.cores = jobCores.toString
  resultFile.mem = jobMem.toString

  sparkContext.parallelize(Seq(resultFile.toString()), 1)
    .saveAsTextFile(s"${sparkJobConfig.jobOutputDir.apply()}/${sparkContext.applicationId}_${sparkJobConfig.jobName.apply()}_out")
}
