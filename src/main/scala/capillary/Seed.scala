package capillary

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.io.Source



class SeedService {


  def getSeedFileDetails(path: String): ListBuffer[SeedFileInfo] = {

    val fileSource = Source.fromFile(path)
    val seedTables: ListBuffer[SeedFileInfo] = new ListBuffer[SeedFileInfo]()

    var headerRead = false
    for (line <- fileSource.getLines()) {

      val data: String = line.trim
      if (data.length > 10) {
        val seedChunks = data.split('|')
        val source_path: String = seedChunks(0)
        val source_format: String = seedChunks(1)
        val target_database_name: String = seedChunks(2)
        val target_table_name: String = seedChunks(3)
        var columnNames: String = null
        if (seedChunks.length > 3)
          columnNames = seedChunks(4)

        if (headerRead) {
          val seedTable = new SeedFileInfo(source_path, source_format, target_database_name, target_table_name, columnNames)
          seedTables += seedTable
        } else
          headerRead = true
      }
    }
    fileSource.close()
    return seedTables;

  }

  def loadSeedData(spark: SparkSession, sc: SparkContext, seedTables: ListBuffer[SeedFileInfo], executionParam: ExecutionParam, batchDetails: BatchDetails, caplogger: AppLogger): Unit = {

    new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("SEED_INFO").withGroup("SEED").withRemark("SEED_PROCESS_START").buildAndLog(caplogger);

    val taskMaster = new TaskMaster(executionParam.parallelThread, null)

    for (seed <- seedTables) {


      val seedTable = seed.target_database_name + "." + seed.target_table_name
      new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("SEED_INFO").withGroup("SEED").withRemarkType("SEED_TABLE_BEFORE_THREAD_START").withRemark(seedTable).buildAndLog(caplogger);

      val capSeedTask = new CapSeedTask(spark = spark, sc = sc, seed = seed, executionParam = executionParam, batchDetails = batchDetails, caplogger = caplogger)
      //      if (taskMaster.status == "ACTIVE")

      taskMaster.submitTask(capSeedTask)
      println(s"Task submitted $seedTable")

    }
    taskMaster.shutItDownNow()
    while (taskMaster.allTaskAreDone() == false) {
      taskMaster.releaseUntilItsDone()
    }


    new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("SEED_INFO").withGroup("SEED").withRemark("SEED_PROCESS_END").buildAndLog(caplogger);
  }
}