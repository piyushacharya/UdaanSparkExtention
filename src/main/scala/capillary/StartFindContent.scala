package capillary

import org.apache.commons.lang.StringUtils

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer

object StartFindContent {
  def main(args: Array[String]): Unit = {

    val de = new DataExtractionService()

    val outFileNameLocation = "/Users/piyush.acharya/MyWorkSpace/Databricks/Projects /03 Capillary /newmetadata20200315/output/consolidated/"
    val orgoutFileNameLocation = "/Users/piyush.acharya/MyWorkSpace/Databricks/Projects /03 Capillary /newmetadata20200315/output/original_consolidated/"
    try {
      new File(outFileNameLocation).delete()

    } catch {
      case e: Exception => println("Some error while deleting... ")
    } finally {
      new File(outFileNameLocation).mkdir()
    }
    try {
      new File(orgoutFileNameLocation).delete()
    } catch {
      case e: Exception => println("Some error while deleting... ")
    } finally {
      new File(orgoutFileNameLocation).mkdir()
    }


    val node_link_loc = "/Users/piyush.acharya/MyWorkSpace/Databricks/Projects /03 Capillary /newmetadata20200315/DAG/neo4j_dump_2021_03_15.csv"
    //    val orgs: List[String] = List("100323")
    val orgs: List[String] = List("100323")
    val executionParam: ExecutionParam = new ExecutionParam(parallelThread = 400, orgs);
    val graphService = new GraphService()
    val batchDetails: BatchDetails = new BatchDetails("local", 1, null, null, null, null, false, false)

    val capQueryModifier = new CapQueryModifier()

    val graph = graphService.loadGraphData(node_link_loc, executionParam)


    var nodes: ListBuffer[Job_node] = graph.get_nodes_for_execution

    var keepgoing = true
    val baseFilePath = "/Users/piyush.acharya/MyWorkSpace/Databricks/Projects /03 Capillary /newmetadata20200315/DAG/Metadata/dag_dump_dir_20210315/"


    val runForOrgans: List[String] = executionParam.runForOrgans
    var queryCount = 0;
    var index = 0

    while (nodes.length > 0 || keepgoing == true) {
      for (n <- nodes) {
        n.status = NodeStatus.Finished
        val nodeJsonPath = n.npath

        if (n.npath != null && "null".equalsIgnoreCase(n.npath) == false) {

          val consolidate_query = de.getdata(baseFilePath + n.npath + ".json")


          val queries = StringUtils.split(consolidate_query, "~~~")

          for (query <- queries) {
            if (query.contains("nps_score") && query.contains("target_100323")) {
              println(query)
              println("*********************")
            }
          }
        }

      }


      nodes = graph.get_nodes_for_execution
      if (nodes.length < 1)
        keepgoing = false;


    }

    println(s"Done processing , total quries to process $queryCount ")

  }
}
