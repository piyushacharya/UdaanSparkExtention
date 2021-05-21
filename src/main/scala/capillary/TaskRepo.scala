package capillary

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import scala.util.Random


class SimpleTestTask() extends GenericTask {
  @throws[Exception]
  override def call: String = {

    try {

      status = TaskStatus.Started;

      if (index % 100 == 0) {
        longJob = true
        System.err.println("Long working thread started index [  " + index + " ] token [ " + token + " ]  long job indicator [ " + longJob + " ]")

        while (true) {
          Thread.sleep(1000)
        }
      }

      val sleepTime = Random.nextInt(5000)
      System.out.println("Task [" + index + "] Token [" + token + "] Sleep Time : " + sleepTime)
      Thread.sleep(sleepTime)
      status = TaskStatus.Finished;
    }
    catch {
      case e: InterruptedException => {
        System.err.println("****************************************************************************************")
        this.synchronized {
          status = TaskStatus.Error;
        }
        System.err.println("Task [" + index + "] Token [" + token + "] Status  InterruptedException  ")
        System.err.println("****************************************************************************************")
      }
    }
    token
  }
}


class CapSeedTask(spark: SparkSession, sc: SparkContext, seed: SeedFileInfo, executionParam: ExecutionParam, batchDetails: BatchDetails, caplogger: AppLogger) extends GenericTask {

  override def call: String = {
    new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("SEED_INFO").withGroup("SEED").withRemarkType("SEED_TABLE_THREAD_START").withRemark(seed.target_database_name + "." + seed.target_table_name).buildAndLog(caplogger);
    try {
      if (seed.source_format == "CSV") {
        createCSVTable(seed = seed)
      } else if (seed.source_format == "PARQUET") {
        createParquetTable(seed = seed)
      }
    }
    catch {
      case e: InterruptedException => {
        System.err.println("****************************************************************************************")
        this.synchronized {
          status = TaskStatus.Error;
        }
        System.err.println("Task [" + index + "] Token [" + token + "] Status  InterruptedException  ")
        System.err.println("****************************************************************************************")
      }

      case e: Throwable => {
        this.synchronized {
          System.err.println(" ****** Task [ " + seed.target_database_name + "." + seed.target_table_name + " ] Token [ " + token + " ] \n\r **Error " + e.getMessage + " ******")
          status = TaskStatus.Error;
          errorMessage = e.getMessage
        }
      }
    }

    status = TaskStatus.Finished
    new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("SEED_INFO").withGroup("SEED").withRemarkType("SEED_TABLE_THREAD_END").withRemark(seed.target_database_name + "." + seed.target_table_name).buildAndLog(caplogger);
    token
  }

  def createParquetTable(seed: SeedFileInfo): Unit = {
    val stmt = s"CREATE TABLE ${seed.target_database_name}.${seed.target_table_name} USING ${seed.source_format} LOCATION '${seed.source_path}'"
    spark.sql(stmt)
  }

  def createCSVTable(seed: SeedFileInfo): Unit = {
    val stmt = s"CREATE TABLE ${seed.target_database_name}.${seed.target_table_name} (${seed.columnNames}) USING ${seed.source_format} LOCATION '${seed.source_path}'"
    spark.sql(stmt)
  }

}


class CapTask(spark: SparkSession, sc: SparkContext, job_node: Job_node, executionParam: ExecutionParam, batchDetails: BatchDetails, caplogger: AppLogger) extends GenericTask {

  def getfromNodeList(): String = {
    val inEdges = job_node.in_edges
    var from_keys = ""
    if (inEdges != null && inEdges.size > 0) {
      {
        for (edge <- inEdges) {
          val jobNode = edge.from_node
          from_keys = from_keys + jobNode.key + "|"
        }
      }
    }
    (from_keys)
  }

  @throws[Exception]
  override def call: String = {

    val task_name = job_node.npath

    val dataExtractionService = new DataExtractionService()
    val capQueryModifier = new CapQueryModifier()
    var runningQuery =""
    try {
      new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(task_name).withGroup("TASK_DETAILS").withType("TASK_STARTED").buildAndLog(caplogger);

      status = TaskStatus.Started;
      job_node.status = NodeStatus.Started
      val baseFilePath = batchDetails.node_folder_loc

      val data = dataExtractionService.getdata(baseFilePath + task_name + ".json")
      // println(data)
      var dummyTask = false;
      if (data != "Dummy") {
        val queries = StringUtils.split(data, "~~~")
        for (query <- queries) {
          new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(task_name).withGroup("TASK_DETAILS").withType("QUERY_STARTED").withQuery(query).buildAndLog(caplogger)
          if (capQueryModifier.isQueryExecutable(batchDetails, query)) {
            val newQuery = capQueryModifier.modifyQuery(query)
            runningQuery= query

            executeQuery(newQuery)
            new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(task_name).withGroup("TASK_DETAILS").withType("QUERY_ENDED").buildAndLog(caplogger)

          } else {
            new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(task_name).withGroup("TASK_DETAILS").withType("QUERY_ENDED").withRemarkType("SKIP_EXECUTION").withRemark("true") buildAndLog (caplogger)
          }
        };

      } else {
        dummyTask = true;
      }


      val childTasks:String = getfromNodeList()
      new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(task_name).withGroup("TASK_DETAILS").withType("TASK_ENDED").withDummy(s"$dummyTask").withIndex(index).buildAndLog(caplogger);

      if (childTasks != "") {
        val lg = new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(task_name).withGroup("TASK_DETAILS").withType("TASK_ENDED_INFO").withRemarkType(childTasks).withRemark("DEP").withIndex(index).withDummy(s"$dummyTask")build()
        caplogger.log(lg)
      };

      status = TaskStatus.Finished;
      job_node.status = NodeStatus.Finished
    }
    catch {
      case e: InterruptedException => {

        this.synchronized {
          status = TaskStatus.Error;
          errorMessage = e.getMessage

          new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(task_name).withGroup("TASK_DETAILS").withType("TASK_QUERY_ERROR").withRemark(e.getMessage).withRemarkType("ERROR").withQuery(runningQuery).buildAndLog(caplogger);
        }

      }
      case e: Throwable => {
        this.synchronized {
          status = TaskStatus.Error;
          errorMessage = e.getMessage
          new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(task_name).withGroup("TASK_DETAILS").withType("TASK_QUERY_ERROR").withRemark(e.getMessage).withRemarkType("ERROR").withQuery(runningQuery).buildAndLog(caplogger);
        }
      }
    }
    token
  }

  def executeQuery(query: String): Unit = {

    if (batchDetails.test == true) {
      val batch_id = batchDetails.batch_id
      println(s"Running query for batch [$batch_id] index # [$index]  " + query)
    } else {
      sc.setLocalProperty("spark.scheduler.pool", token)
      sc.setLocalProperty("spark.scheduler.pool", "description - "+ token)
      val df = spark.sql(query)
    }


  }
}


class SimpleSparkTestTask(spark: SparkSession, sc: SparkContext, job_node: Job_node, executionParam: ExecutionParam, batchDetails: BatchDetails, caplogger: AppLogger) extends GenericTask {
  @throws[Exception]
  override def call: String = {

    val task_name = job_node.key

    val dataExtractionService = new DataExtractionService()


    var currQuery: String = null;
    try {
      status = TaskStatus.Started;
      job_node.status = NodeStatus.Started

      currQuery = job_node.data
      new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(job_node.key).withGroup("TASK_DETAILS").withType("TASK_START").buildAndLog(caplogger);
      new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(job_node.key).withGroup("TASK_DETAILS").withType("TASK_QUERY_START").buildAndLog(caplogger);
      executeQuery(currQuery)
      new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(job_node.key).withGroup("TASK_DETAILS").withType("TASK_QUERY_END").buildAndLog(caplogger);
      new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(job_node.key).withGroup("TASK_DETAILS").withType("TASK_END").buildAndLog(caplogger);
      status = TaskStatus.Finished;
      job_node.status = NodeStatus.Finished
    }
    catch {
      case e: InterruptedException => {
        System.err.println("****************************************************************************************")
        this.synchronized {
          status = TaskStatus.Error;
          job_node.status = NodeStatus.Error
          errorMessage = e.getMessage
        }
        System.err.println("Task [" + task_name + "] Token [" + token + "] Status  InterruptedException  ")
        System.err.println("****************************************************************************************")
        new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(job_node.key).withGroup("TASK_DETAILS").withType("TASK_QUERY_ERROR").withRemark(e.getMessage).withQuery(currQuery).buildAndLog(caplogger);
      }


      case e: Throwable => {
        this.synchronized {
          System.err.println(" ****** Task [ " + task_name + " ] Token [ " + token + " ] \n\r Query ** " + currQuery + "  InterruptedException  \n\r **Error " + e.getMessage + " ******")
          status = TaskStatus.Error;
          job_node.status = NodeStatus.Error
          errorMessage = e.getMessage
          new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(job_node.key).withGroup("TASK_DETAILS").withType("TASK_QUERY_ERROR").withRemark(e.getMessage).withQuery(currQuery).buildAndLog(caplogger);
        }
      }
    }
    token
  }

  def executeQuery(query: String): Unit = {
    println(index + " # node ,Executing query " + query)
    sc.setLocalProperty("spark.scheduler.pool", token)
    sc.setLocalProperty("spark.scheduler.pool", "description - "+ token)
    val df = spark.sql(query)
//    println("Plan for Execution :" + df.queryExecution.executedPlan.id)
  }

}




