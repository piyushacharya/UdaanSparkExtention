package capillary

import java.io.File

object StartGraphDep {
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
    print(graph)

    // graph.get_siblling_nodes("16918_org100323LookupTableLookupTable_from_nsadmin_messages_nsadmin_messages_n6091kpicomputephase100323d67b88bf-64da-4551-bc34-498d4f9600a7")
//    graph.get_all_parents_nodes("16918_org100323LookupTableLookupTable_from_nsadmin_messages_nsadmin_messages_n6091kpicomputephase100323d67b88bf-64da-4551-bc34-498d4f9600a7")
    val queryNode: Job_node = graph.get_job_node_by_name("16918_CreateOrgLevelTablesfortableusers_ndnc_statustruepartitionedtablesallorgscreateHiveTablesuser_management10550fa7-48fc-4fdf-b874-3c694ca1f0e8")
    val dbNode: Job_node = graph.get_job_node_by_name("16918_CreatingdummyTableforuser_managementcreateHiveTablesuser_management8e55824f-626b-48a2-ae42-869e81213008")

    println("Wait ")
  }
}


