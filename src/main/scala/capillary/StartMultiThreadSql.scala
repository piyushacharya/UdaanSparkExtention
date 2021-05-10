package capillary

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source
import scala.collection.mutable.ListBuffer
import java.io.File
import org.apache.spark.SparkContext


object StartMultiThreadSql {

  class MsckRunner(sc: SparkContext, spark: SparkSession, task: ListBuffer[String], fpindex: Int) extends Runnable {

    var status: String = "New"

    override def run() {
      try {
        status = "START"
        for (sqlLine <- task) {
          if (sqlLine.isEmpty != "" && sqlLine.length > 5) {

            println(s"Rinning sql $fpindex =>")
            //            sc.setLocalProperty("spark.scheduler.pool", fpindex + "")
            //            sc.setLocalProperty("spark.scheduler.pool", "description - " + fpindex + "")
            //spark.sql(sqlLine)
          }
        }

        status = "FINISHED"

      }
      catch {

        case e: Throwable => {
          status = "ERROR"
          println("Error " + e.getMessage)
        }
      }
    }
  }

  class ExecuteSQL() {
    def executeSQL(linkfilename: String): Unit = {
      var run = true
      val msckRepairRunners: mutable.Map[String, Thread] = new mutable.HashMap[String, Thread]()
      val msckRepairRunnersCon: mutable.Map[String, MsckRunner] = new mutable.HashMap[String, MsckRunner]()
      val tasks: mutable.Queue[mutable.ListBuffer[String]] = new mutable.Queue[mutable.ListBuffer[String]]()

      val poolSize = 3;
      val msckTargetTables = scala.collection.mutable.Queue.empty[String]


      var task = new mutable.ListBuffer[String]
      for (line <- Source.fromFile(linkfilename).getLines()) {
        if (line.startsWith("-------")) {
          tasks += task
          task = new mutable.ListBuffer[String]
        } else {
          task += line

        }
      }

      println(s"Total tasks  ${tasks.size}")

      var index = 0
      while (run) {

        if (msckRepairRunners.size < poolSize) {
          while (msckRepairRunners.size < poolSize && tasks.size > 0) {
            index = index + 1
            val fairPoolIndex = index % poolSize
            val t1 = tasks.dequeue()
            val msckRunner: MsckRunner = new MsckRunner(null, null, t1, fairPoolIndex)
            var t = new Thread(msckRunner)
            t.start()
            val id = java.util.UUID.randomUUID.toString
            msckRepairRunners += (id -> t)
            msckRepairRunnersCon += (id -> msckRunner)
          }
        }

          val tempList: ListBuffer[String] = scala.collection.mutable.ListBuffer.empty[String]

          for ((k, v) <- msckRepairRunners) {
            if (v.isAlive == false) {
              tempList += k
              val msckRunner: MsckRunner = msckRepairRunnersCon.get(k).get
              if (msckRunner.status == "ERROR")
                run = false;
            }
          }
          if (tempList.size > 0) {
            println("Released " + tempList.size)
            for (k <- tempList) {
              msckRepairRunners.remove(k)
              msckRepairRunnersCon.remove(k)
            }
          }


        if (tasks.size <= 0 && msckRepairRunners.size <= 0 && msckRepairRunnersCon.size <= 0)
          run = false;

      }

    }

  }


  def main(args: Array[String]): Unit = {


    val linkfilename = "/Users/piyush.acharya/MyWorkSpace/Databricks/Projects /03 Capillary /newmetadata20200315/output/consolidated/1.sql"

    val executeSQL = new ExecuteSQL()
    executeSQL.executeSQL(linkfilename)

  }

}
