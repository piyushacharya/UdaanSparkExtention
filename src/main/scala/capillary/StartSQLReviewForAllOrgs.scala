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
        n.status = NodeStatus.Finished

        /*var pathConsider = false
        val orgIndex = StringUtils.indexOf(n.npath, orgnId)
        if (orgIndex > 0) {
          val preChar = n.npath.charAt(orgIndex - 1)
          val postChar = n.npath.charAt(orgIndex + StringUtils.length(orgnId))
          if ((preChar > 47 && preChar < 58) || (postChar > 47 && postChar < 58))
            pathConsider = false
          else
            pathConsider = true
        }*/

        /*
        PreTransformfortablegroup_detailsForDbcampaign_
        PreTransformfortableorgCreateOrgLevelTablesfortable
        * */

        //        if (n.npath != null  && n.npath.contains("16918_create_view_dim_64_org_100323dim_target_table_creation100323bff7849d-6023-4943-b420-45b326b4294e")  )
        //          println("looking into ....")

       /* if (n.npath != null && n.npath.contains(s"attribution$orgnId"))
          pathConsider = true

        var createRrpreTransformforFile = false
        if (n.npath != null && (n.npath.contains("Createpredimd") || n.npath.contains("Createpre") || n.npath.contains("Creatingdummy") || n.npath.contains("CreateRepartitioned")  || n.npath.contains("CreateDB_transpose") )) {
          pathConsider = true
        }else if (n.npath != null && ( n.npath.contains("PreTransformfororg") ||  n.npath.contains("CreateOrgLevelTablesfortable") ||  n.npath.contains("PreTransformfortableinventory")  )) {
          pathConsider = true
          createRrpreTransformforFile = true;
        }else if (n.npath != null && ( n.npath.contains("_transpose") )) {
          pathConsider = true
          createRrpreTransformforFile = true;
        }*/

        // && pathConsider == true
        if (n.npath != null && "null".equalsIgnoreCase(n.npath) == false ) {


          val nodeJsonPath = n.npath

          val consolidate_query = de.getdata(baseFilePath + n.npath + ".json")
          var dummyTask = false;
          if ("select 1".equalsIgnoreCase(consolidate_query)) {
            println("Stop for Inspection")
            de.getdata(baseFilePath + n.npath + ".json")
          }
          if (consolidate_query !=
            "Dummy") {
            val queries = StringUtils.split(consolidate_query, "~~~")
            var queryAddedInfile = false
            for (query <- queries) {

              if (capQueryModifier.isQueryExecutable(batchDetails, query)) {
                val newQuery = capQueryModifier.modifyQuery(query)

                var addQuery = true

                /*if (createRrpreTransformforFile == false) {
                  addQuery = true
                } else if (createRrpreTransformforFile == true && query.contains(orgnId)) {
                  addQuery = true
                }else if (createRrpreTransformforFile == true && capQueryModifier.isSplCondition(query)) {
                  addQuery = true
                }*/


                if (addQuery == true && writer == null && orgwriter == null) {
                  writer = new PrintWriter(new File(outFileNameLocation + index + ".sql"))
                  orgwriter = new PrintWriter(new File(orgoutFileNameLocation + index + ".sql"))
                  index = index + 1
                }
                if (addQuery)
                {
                  println(newQuery)
                  orgwriter.write(query + ";\n\r")
                  writer.write(newQuery + ";\n\r")
                  queryCount = queryCount + 1
                  queryAddedInfile = true
                }

              }


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
