package capillary

import org.apache.commons.lang.StringUtils

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer

object StartSQLReviewForAllOrgs {

  def main(args: Array[String]): Unit = {

    val de = new DataExtractionService()

    // val projectBasePath = "/Users/piyush.acharya/MyWorkSpace/Databricks/Projects /03 Capillary /"
    val projectBasePath = "/Users/vijay.pavan/IdeaProjects/03.Capillary/"

    val outFileNameLocation = projectBasePath + "newmetadata20200315/output/consolidated/"
    val orgoutFileNameLocation = projectBasePath + "newmetadata20200315/output/original_consolidated/"

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


    val node_link_loc = projectBasePath + "newmetadata20200315/DAG/neo4j_dump_2021_03_15.csv"
    //    val orgs: List[String] = List("100323")
    val orgs: List[String] = List() // List("100323")
    val executionParam: ExecutionParam = new ExecutionParam(parallelThread = 400, orgs);
    val graphService = new GraphService()
    val batchDetails: BatchDetails = new BatchDetails("local", 1, null, null, null, null, false, false)

    val otherUtils = new OtherUtils
    val capQueryModifier = new CapQueryModifier()

    val graph = graphService.loadGraphData(node_link_loc, executionParam)


    var nodes: ListBuffer[Job_node] = graph.get_nodes_for_execution

    var keepgoing = true
    val baseFilePath = projectBasePath + "newmetadata20200315/DAG/Metadata/dag_dump_dir_20210315/"


    val runForOrgans: List[String] = executionParam.runForOrgans
    var queryCount = 0;
    var index = 0
    // val orgnId = "100323"
    while (nodes.length > 0 || keepgoing == true) {
      var writer: PrintWriter = null
      var orgwriter: PrintWriter = null

      for (n <- nodes) {
        if (otherUtils.isDeadEndNode(n))
          n.status = NodeStatus.dead_end
        else {
          n.status = NodeStatus.Finished

          if (n.npath != null && "null".equalsIgnoreCase(n.npath) == false) {

            val nodeJsonPath = n.npath

            val consolidate_query = de.getdata(baseFilePath + n.npath + ".json")
            var dummyTask = false;
            if ("select 1".equalsIgnoreCase(consolidate_query)) {
              println("Stop for Inspection")
              de.getdata(baseFilePath + n.npath + ".json")
            }
            if (consolidate_query != "Dummy") {

              /*if (consolidate_query.contains("pid_slab_number_map")) {
                println("debugger")
              }*/

              val queries = StringUtils.split(consolidate_query, "~~~")
              var queryAddedInfile = false
              for (query <- queries) {

                if (consolidate_query.contains("masters__hive_intermediate_16918__incr.stores_add_ons")) {
                  println("debugger")
                }

                if (capQueryModifier.isQueryExecutable(batchDetails, query.trim)) {
                  val newQuery = capQueryModifier.modifyQuery(query.trim)

                  val addQuery = true


                  if (addQuery == true && writer == null && orgwriter == null) {
                    writer = new PrintWriter(new File(outFileNameLocation + index + ".sql"))
                    orgwriter = new PrintWriter(new File(orgoutFileNameLocation + index + ".sql"))
                    index = index + 1
                  }
                  if (addQuery) {
                    println(newQuery)
                    orgwriter.write(query + ";\n\r")
                    writer.write(newQuery + ";\n\r")
                    queryCount = queryCount + 1
                    queryAddedInfile = true
                  }
                } // end if isQueryExecutable


              }
              if (queryAddedInfile) {
                writer.write("-----------------------" + n.npath + "-----------------------\n\r")
                orgwriter.write("-----------------------" + n.npath + "-----------------------\n\r")
                writer.flush()
                orgwriter.flush()
                println("-----------------------" + n.npath + "-----------------------")
              }


            }
          }
        }
      }

      nodes = graph.get_nodes_for_execution
      if (nodes.length < 1)
        keepgoing = false;

      if (orgwriter != null && writer != null) {
        writer.close()
        orgwriter.close()
      }


    }

    println(s"Done processing , total queries to process $queryCount ")
  }
}
