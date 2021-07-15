package samples

import scala.io.Source

object ExecutionLogExtractor {

  def main(args: Array[String]): Unit = {
    extractLogsInfo
  }


  def extractLogsInfo(): Unit = {
    // eu_test1_logs.txt
    // RunIdTag = db_run_eu_metatest5_8  _6w_meta4  RunBook/
    val fileName = "/Users/vijay.pavan/IdeaProjects/03.Capillary/4x/4x_execution_logs.txt"
    val lines = Source.fromFile(fileName).getLines()
    lines.foreach(line => {
      if (line.contains("Total tasks") ) {
        val taskNo = line.substring(line.indexOf("Total tasks "), line.indexOf("Task Tokens "))
          .replace("Total tasks ", "")
        print(s"${taskNo}")
      }
    if (line.contains("Finished Executing") ) {
      // println(s"Processing $line")
      val indx = if (line.contains("seconds or")) line.indexOf("' seconds or '") else line.indexOf("' secs or '")
      val secs = line.substring(line.indexOf(" '") + 1, indx).replace("'", "")
      val mins = line.substring(indx, line.indexOf("' mins")).replace("' seconds or '", "")
        .replace("' secs or '", "")
      println(s"${secs}, ${mins}")
    }
    })
  }

}