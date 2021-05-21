package capillary


import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import spray.json.JsString

import scala.collection.mutable.{ListBuffer, Map}
import spray.json._

import scala.io.Source
import org.apache.spark.sql.SparkSession

import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable


case class BatchDetails(spark_mode: String, batch_id: Long, seed_rel_file_loc: String, seed_file_loc: String, node_link_loc: String, node_folder_loc: String, restart: Boolean, test: Boolean)

case class ExecutionParam(parallelThread: Int, runForOrgans: List[String])

case class SeedFileInfo(source_path: String, source_format: String, target_database_name: String, target_table_name: String, columnNames: String)

class MyListener extends SparkListener {

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println("Job id *********** " + jobStart.jobId + " *********** job.description " + jobStart.properties.get("spark.job.description") + " *********** jobGroup.id " + jobStart.properties.get("spark.jobGroup.id"))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("Start " + jobEnd.jobId + " Result" + jobEnd.jobResult)
  }
}


class AuxiliaryServices {

  def getLatestBathcId(): Long = {
    return 0;
  }

  def storeBatchInfo(batchDetails: BatchDetails): Unit = {
    return 0;
  }

  def getSeedInfo(batch_id: Long): String = {
    return "SEED_DONE"
  }
}

class RegisterSeedTable(spark: SparkSession, file_location: String, rel_doc_path: String, poolSize: Int) {

  val pool: ExecutorService = Executors.newFixedThreadPool(poolSize)

  def register(): Unit = {

  }
}


class CapillaryDataLoader {


  private def createExternalTables(spark: SparkSession, file_location: String, rel_doc_path: String, restart: Boolean, batch_id: Long): Unit = {

  }

  // create external table
  def loadSeedData(spark: SparkSession, sc: SparkContext, seedTables: ListBuffer[SeedFileInfo], executionParam: ExecutionParam, batchDetails: BatchDetails, caplogger: AppLogger): Unit = {


    val seedService = new SeedService
    seedService.loadSeedData(spark = spark, sc = sc, seedTables = seedTables, executionParam = executionParam, batchDetails = batchDetails, caplogger = caplogger)

  }

  def loadGraph(node_link_loc: String, caplogger: AppLogger, executionParam: ExecutionParam): Graph = {
    val graphService = new GraphService()
    val graph = graphService.loadGraphData(node_link_loc, executionParam)
    return graph;

  }

  def markGraphNodeCompleted(graph: Graph, compledTask: ListBuffer[String]): Long = {
    var index: Long = 0
    for (key <- compledTask) {
      val node = graph.get_node(key)
      node.status = NodeStatus.Finished
      index = index + 1
    }
    (index)
  }

  def executeGraph(spark: SparkSession, sc: SparkContext, graph: Graph, caplogger: AppLogger, batchDetails: BatchDetails, executionParam: ExecutionParam): Int = {
    val graphService = new GraphService()
    return graphService.executeGraph(graph = graph, sc = sc, poolSize = 100, spark = spark, batchDetails: BatchDetails, executionParam = executionParam, caplogger = caplogger)

  }


}

class DataExtractionService {

  def getdata(fileName: String): String = {

    val fSource = Source.fromFile(fileName)

    //  val lines: String = scala.io.Source.fromFile(fullFilePath).mkString.toString
    val lines: String = fSource.mkString
    fSource.close()

    // lines.contains("com.capillary.reon.workflow.FinalCreatePreHook") &&

    /*if ( lines.contains("dimension_src_merged_100359.subscription_transpose_100359_merged_temp") )
    {
      println("\nfileName --> " + fileName.substring(fileName.indexOf("Capillary")))
    }*/



    val jsonAst = lines.parseJson

    val tmp = jsonAst.asJsObject()
    val key = tmp.fields("key")



    if (fileName.contains("16918_orgdropCreateDatabasesnew_kpi_compute_100323attribution1003235d15cc35-dc04-4b8d-8ddb-2649a06aa978")) {
      println("stop for inspection")
    }

    val level1PropertyFields = tmp.fields.get("properties").get.asJsObject().fields


    val level2PropertyFields = tmp.fields.get("properties").get.asJsObject().fields.get("properties").get.asJsObject().fields
    val level3PropertyFields = level2PropertyFields.get("properties").get.asJsObject().fields
    var stmt: String = null
    if (level3PropertyFields.contains("query")) {
      stmt = level3PropertyFields.get("query").get.asInstanceOf[JsString].value
      if (stmt != null)
        stmt = stmt.trim

      if (stmt.toUpperCase.startsWith("SELECT 1") && level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "") {
        stmt = "Dummy"
      } else if (stmt.toUpperCase.startsWith("SELECT 1") && level3PropertyFields.contains("dynamicSparkPosthookClassName") && level3PropertyFields.get("dynamicSparkPosthookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.commons.executors.posthooks.PersistFactSchemaToMeta") {
        stmt = "Dummy"
      }
      else if (stmt.toUpperCase.startsWith("SELECT 1") && level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.dimension_builder.querygen.persistance.DimTargetTablePreHook") {
        val db_name = level3PropertyFields.get("DATABASE_NAME").get
        val table_name=level3PropertyFields.get("TABLE_NAME").get
        val view_name = level3PropertyFields.get("VIEW_NAME").get
        stmt = "CREATE OR REPLACE TABLE " + db_name + "."+table_name +" USING DELTA AS SELECT * from " + db_name+"." +view_name
        stmt=stmt.replaceAll("\"","")
      }
      else if (stmt.toUpperCase.startsWith("SELECT 1") && level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.workflow.FinalCreatePreHook") {
        val db_name = level3PropertyFields.get("DATABASE_NAME").get
        val table_name=level3PropertyFields.get("TABLE_NAME").get
        val view_name = level3PropertyFields.get("DDL_VIEW_NAME").get
        stmt = "CREATE OR REPLACE TABLE " + db_name + "."+table_name +" USING DELTA AS SELECT * from " + db_name+"." +view_name
        stmt=stmt.replaceAll("\"","")
      }
      else if (stmt.toUpperCase.startsWith("SELECT 1") && level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.summary_kpi.core.TableWriteTaskPreHook") {
        val db_name = level3PropertyFields.get("DATABASE_NAME").get
        val table_name=level3PropertyFields.get("TABLE_NAME").get
        val view_name = level3PropertyFields.get("DDL_VIEW_NAME").get
        stmt = "CREATE OR REPLACE TABLE " + db_name + "."+table_name +" USING DELTA AS SELECT * from " + db_name+"." +view_name
        stmt=stmt.replaceAll("\"","")
      }
      else if (stmt.toUpperCase.startsWith("SELECT 1") && level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.sqoop.executors.RetrieveS3TaskPreHook") {
        stmt = "Dummy"
      }
      else if (stmt.toUpperCase.startsWith("SELECT 1") && level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.sqoop.executors.RetrieveS3TaskPreHook") {
        stmt = "Dummy"
      }
      else if (stmt.toUpperCase.startsWith("SELECT 1") && level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.etl_execution.strategies.RetrieveEventFactTaskPreHook") {
        stmt = "Dummy"
      }

      else if (stmt.toUpperCase.startsWith("SELECT 1") && level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.etl_execution.strategies.RetrieveEventFactTaskPreHook") {
        stmt = "Dummy"
      }

      else if (level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.etl_execution.strategies.RetrieveEventFactTaskPreHook") {
        stmt = "Dummy"
      }
      else if (level3PropertyFields.contains("dynamicSparkPrehookClassName") && level3PropertyFields.get("dynamicSparkPrehookClassName").get.asInstanceOf[JsString].value == "com.capillary.reon.sqoop.executors.SqoopCdmDMLPreHook") {
        stmt = "Dummy"
      }

      //TODO : Blanket Dummy based on select need to confirm with Satish
      else if (stmt.toUpperCase.startsWith("SELECT 1")) {
        stmt = "Dummy"
      }
      else if (stmt.equalsIgnoreCase("SELECT 1")) {
        stmt = "Dummy"
      }
      else if (level3PropertyFields.contains("MY-DOT-sql")) {
        stmt = level3PropertyFields.get("MY-DOT-sql").get.asInstanceOf[JsString].value
      } else if (stmt.toUpperCase.contains("SELECT 1") && level3PropertyFields.contains("TABLE_NAME") && level3PropertyFields.contains("DATABASE_NAME")) {
        val dbName = level3PropertyFields.get("DATABASE_NAME").get.asInstanceOf[JsString].value
        val tableName = level3PropertyFields.get("TABLE_NAME").get.asInstanceOf[JsString].value
        val viewName = level2PropertyFields.get("name").get.asInstanceOf[JsString].value
        stmt = "CREATE or replace table " + dbName + "." + tableName + " using delta as select * from " + dbName + "." + viewName
      } else if (stmt.toUpperCase.contains("SELECT 1") && level3PropertyFields.contains("HISTORY_DATABASE_NAME") && level3PropertyFields.contains("HISTORY_TABLE_NAME") && level3PropertyFields.contains("TRANS_TABLE_NAME")) {
        val dbName = level3PropertyFields.get("HISTORY_DATABASE_NAME").get.asInstanceOf[JsString].value
        val tableName = level3PropertyFields.get("HISTORY_TABLE_NAME").get.asInstanceOf[JsString].value
        val viewName = level3PropertyFields.get("TRANS_TABLE_NAME").get.asInstanceOf[JsString].value
        stmt = "CREATE or replace table " + dbName + "." + tableName + " using delta as select * from " + dbName + "." + viewName
      }
    }
    else
      stmt = "Dummy"


    // Some more Cheks based on phaseProps classes for sqoop
    if (tmp.fields.get("properties").get.asJsObject().fields.contains("phaseProps")) {
      val level1phasePropsFields = tmp.fields.get("properties").get.asJsObject().fields.get("phaseProps").get.asJsObject().fields
      if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value == "com.capillary.reon.sqoop.executors.SqoopPhaseExecutor")
        stmt = "Dummy"
      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value == "com.capillary.reon.dimension_builder.executors.DimValueSqoopPE")
        stmt = "Dummy"
      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value == "com.capillary.reon.sqoop.executors.SqoopESPE")
        stmt = "Dummy"
      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value == "com.capillary.reon.sqoop.executors.SqoopPartitionPhaseExecutor")
        stmt = "Dummy"
      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value.contains("com.capillary.reon.sqoop.executors.RetrieveAndFullIncrementalUnificationPhaseExecutor"))
        stmt = "Dummy"
      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value.contains("com.capillary.reon.dimension_builder.executors.EventDimValueImportPE"))
        stmt = "Dummy"
      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value.contains("com.capillary.reon.etl_execution.core.EventFactEtlPhaseExecutor"))
        stmt = "Dummy"
      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value.contains("com.capillary.reon.sqoop.executors.RetrieveAndFullIncrementalUnificationPhaseExecutor"))
        stmt = "Dummy"
//      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value.contains("com.capillary.reon.dimension_builder.executors.PreDimPhase"))
//        stmt = "Dummy"
//      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value.contains("com.capillary.reon.sqoop.executors.CreateHiveTablesDbContextPhaseExecutor"))
//        stmt = "Dummy"
//      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value.contains("com.capillary.reon.sqoop.executors.TransposeTablePhaseExecutor"))
//        stmt = "Dummy"
//      else if (level1phasePropsFields.contains("className") && level1phasePropsFields.get("className").get.asInstanceOf[JsString].value.contains("com.capillary.reon.dimension_builder.executors.DimTransformerPE"))
//        stmt = "Dummy"




    }


    return stmt;

  }


}

class StartLoad(sc: SparkContext, spark: SparkSession, executionParam: ExecutionParam, caplogger: AppLogger) {

  val locations: mutable.Map[String, String] = new mutable.HashMap[String, String]()
  val cdl: CapillaryDataLoader = new CapillaryDataLoader
  val seedSerive = new SeedService


  def addLocation(loc: String, key: String): Unit = {
    locations += (key -> loc)
  }


  def startLoad(batch_id: Long, test: Boolean): Unit = {


    var curr_batch_id = batch_id;


    val seed_rel_file_loc = locations.get("seed_rel_file_loc").get
    val seed_files_loc = null
    val node_link_loc = locations.get("node_link_loc").get
    val node_folder_loc = locations.get("node_folder_loc").get

    var restart = false;

    if (curr_batch_id == 0)
      curr_batch_id = caplogger.getLatestBathcId() + 1L
    else
      restart = true

    println(s"Batch id [$curr_batch_id]")
    val batchDetails: BatchDetails = new BatchDetails("local", curr_batch_id, seed_rel_file_loc, seed_files_loc, node_link_loc, node_folder_loc, restart, test)
    caplogger.storeBatchDetails(batchDetails = batchDetails, executionParam = executionParam)


    //    val seedTables: ListBuffer[SeedFileInfo] = seedSerive.getSeedFileDetails(seed_rel_file_loc)
    //    cdl.loadSeedData(spark = spark, sc = sc, seedTables = seedTables, executionParam = executionParam, batchDetails = batchDetails, caplogger = caplogger)


    new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("GRAPH_INFO").withGroup("GRAPH").withRemark("GRAPH_STARTED").buildAndLog(caplogger);
    val graph = cdl.loadGraph(batchDetails.node_link_loc, caplogger, executionParam)

    println("graph:: " + graph)

    if (restart) {
      val complted_task_ids = caplogger.getCompltedTasks(curr_batch_id)
      println(s"Task Completed found ${complted_task_ids.size}")
      val finishedCount = cdl.markGraphNodeCompleted(graph, complted_task_ids)
      println(s"Task Completed marked $finishedCount")
    }


    val nodeSentForExecutionCount = cdl.executeGraph(spark = spark, sc = sc, graph = graph, caplogger = caplogger, batchDetails = batchDetails, executionParam = executionParam)
    println(s"Graph done with nodes : $nodeSentForExecutionCount")

    new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("GRAPH_INFO").withGroup("GRAPH").withRemark("GRAPH_END").withRemarkType(nodeSentForExecutionCount + "").buildAndLog(caplogger);

  }


}


class CapQueryModifier() {

  def getDateSecondColumn(sqlQuery: String): String = {
    sqlQuery.substring(sqlQuery.lastIndexOf(',') + 1, sqlQuery.lastIndexOf(')')).trim
  }

  def getDateAddFirstPart(sqlQuery: String): String = {
    sqlQuery.substring(0, sqlQuery.lastIndexOf(',') ).trim
  }

  def parseDateAddFunction(sqlQuery: String): String = {
    String.format("%1$s, CAST(%2$s AS INT) )", getDateAddFirstPart(sqlQuery) , getDateSecondColumn(sqlQuery))
  }

  def modifyQuery(query: String): String = {

    var newQuery= query

    // for create table

    if (newQuery.startsWith("CREATE TABLE") &&  newQuery.contains("IF NOT EXISTS") == false)
      newQuery= newQuery.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS ")



    if (newQuery.toUpperCase().contains("DELTA")) {
      (newQuery)
    } else if (newQuery.length > 0) {
      if (newQuery.contains("USING PARQUET"))
        return newQuery.replace("USING PARQUET", "USING DELTA ")

      if (newQuery.contains("STORED AS PARQUET SELECT") || newQuery.contains("STORED AS PARQUET AS") )
        return newQuery.replace("STORED AS PARQUET ", "USING DELTA ")

      if (newQuery.contains("STORED AS PARQUET SELECT"))
        return newQuery.replace("STORED AS PARQUET ", "USING DELTA AS ")

      if (newQuery.contains("date_add(")) {
        val pattern: scala.util.matching.Regex = "[(a-z_0-9\\s,.'%\\-)]+\\)(?=\\s[a-zA-Z]+\\sNULL\\send)".r
        for (mtch <- pattern.findAllMatchIn(newQuery)) {
          val matchedStr = mtch.toString()
          // println("---->" + matchedStr + "\n -->" + parseDateAddFunction(matchedStr))
          newQuery = newQuery.replace(matchedStr, parseDateAddFunction(matchedStr))
        }
        return newQuery
      }

      if (newQuery.startsWith("CREATE TABLE IF NOT EXISTS") && newQuery.contains("auto_update_time")) {
        newQuery = newQuery.replace("`auto_update_time` bigint", "`auto_update_time` string")
        return newQuery
      }
    }
    (newQuery)
  }

  def isQueryExecutable(batchDetails: BatchDetails, query: String): Boolean = {
    if (batchDetails != null && !batchDetails.restart) {
      if (query.toUpperCase().startsWith("DROP TABLE IF EXISTS"))
        return false;
      else if (query.toUpperCase().startsWith("REFRESH TABLE "))
        return false;
    }
    if (query.toUpperCase().startsWith("ANALYZE TABLE ")) {
      return false;
    }
    if (query.toUpperCase().startsWith("UPDATE ")) {
      return false;
    }
    if (query.toUpperCase().startsWith("SET SPARK.SQL")) {
      return false;
    }
    if (query.toUpperCase().startsWith("DROP VIEW IF EXISTS")) {
      return false;
    }
    if (query.toUpperCase().startsWith("SHOW CREATE TABLE") ||
      query.toUpperCase().startsWith("DESCRIBE EXTENDED")) {
      return false;
    }
      (true)
  }

  def isSplCondition( query: String): Boolean = {
    if (query.startsWith("CREATE TABLE") && query.contains("__hive_intermediate_16918__full"))
      return true

    if (query.startsWith("CREATE TABLE") && query.contains("__pre_dim_intermediate_16918"))
      return true

    if (query.startsWith("CREATE DATABASE") && query.contains("__pre_dim_intermediate_16918"))
      return true

    return false
  }
}
