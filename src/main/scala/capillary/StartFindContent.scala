package capillary

import org.apache.commons.lang.StringUtils

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer

object StartFindContent {
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
    val orgs: List[String] = List("100323")
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

    while (nodes.length > 0 || keepgoing == true) {
      for (n <- nodes) {
        n.status = NodeStatus.Finished
        val nodeJsonPath = n.npath

        if (n.npath != null && "null".equalsIgnoreCase(n.npath) == false) {

          val consolidate_query = de.getdata(baseFilePath + n.npath + ".json")
          val queries = StringUtils.split(consolidate_query, "~~~")

          for (query <- queries) {
            //&& query.toLowerCase().contains("target_100323")  MATERIALIZED,  attribution_100323
            // new_kpi_compute_100323.ResultTable_from_JoinedTable_L_1_coupons_coupons_d_4f29b2dcebaf4
            // ead_api_100323.response_bill_summary;
            // read_api_100323.response_bill_summary__view
            // dimension_src_merged_100425.loyaty_txn_extended_fields_transpose_100425_merged
            // profile_v2_comm_channels_mongo_doc_transpose_100507_merged__view
            // target_100581`.`store_custom_fields
            // read_api_100292`.`customer_summary
            if (query.toLowerCase().contains("customer_summary".toLowerCase()) // ){
              && query.toLowerCase().contains("read_api_100292") ) {
              // println(query)
              println("\n*********************" + n.npath)
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
