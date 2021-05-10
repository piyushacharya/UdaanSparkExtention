package capillary

import java.util.Date
import scala.collection.mutable.{ListBuffer, Map}
import java.util.Calendar
import scala.collection.mutable
import scala.util.control.Breaks._
import spray.json._
import DefaultJsonProtocol._

import scala.io.Source
import java.io.File
import java.io.PrintWriter
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.util.concurrent.{ExecutorService, Executors}

object NodeStatus extends Enumeration {
  val None, init, Started, Finished, Error = Value
}

class Graph {

  var nodes: Map[String, Job_node] = Map()
  var edges: Map[String, JobEdge] = Map()
  var status = "Active"

  def add_node(node: Job_node): Job_node = {
    if (nodes.contains(node.key)) {
      return nodes(node.key)
    } else {
      nodes += (node.key -> node)
      return node;
    }


  }


  def add_Edge(node1: Job_node, node2: Job_node): Unit = {
    val edge_id = java.util.UUID.randomUUID.toString;
    val edge = JobEdge(edge_id, node1, node2)
    node1.out_edges += edge
    node2.in_edges += edge
    edges += (edge.edge_id -> edge);
  }


  def get_node(sample: String): Job_node = {
    if (!nodes.contains(sample))
      return null;
    return nodes(sample)
  }


  def get_edge(edge_id: String): JobEdge = return edges(edge_id)

  def get_level1_node(status: String): ListBuffer[Job_node] = {
    var tmpnodes = ListBuffer[Job_node]()
    for (v <- nodes.values) {
      if (v.in_edges.length < 1)
        tmpnodes += v

    }
    return tmpnodes

  }


  def get_nodes_for_execution: ListBuffer[Job_node] = {
    var tmpnodes = ListBuffer[Job_node]()
    for (v <- nodes.values) {
      if (v.status == NodeStatus.None) {
        if (v.in_edges.length < 1) {
          tmpnodes += v
        }
        else if (all_previous_done_nodes(v) == true) {
          tmpnodes += v
        }
      }

    }
    return tmpnodes
  }

  def marked_finished(job_node: Job_node): Unit = {
    job_node.end_time = Calendar.getInstance().getTime()
    updateNodeStatus(job_node, NodeStatus.Finished)
  }

  def marked_error(job_node: Job_node): Unit = {
    job_node.end_time = Calendar.getInstance().getTime()
    updateNodeStatus(job_node, NodeStatus.Finished)
  }

  def marked_started(job_node: Job_node): Unit = {
    job_node.start_time = Calendar.getInstance().getTime()
    updateNodeStatus(job_node, NodeStatus.Finished)
  }

  def get_all_started_nodes: ListBuffer[Job_node] = {
    var tmpnodes = ListBuffer[Job_node]()
    for (v <- nodes.values) {
      if (v.status == NodeStatus.Started) {
        tmpnodes.append(v)
      }
    }
    return tmpnodes
  }

  def get_level1_nodes: ListBuffer[Job_node] = {
    var tmpnodes = ListBuffer[Job_node]()
    for (v <- nodes.values) {
      if (v.in_edges.length < 1) {
        tmpnodes.append(v)
      }
    }
    return tmpnodes

  }

  def get_next_nodes(job_node: Job_node): ListBuffer[Job_node] = {
    var tmpnodes = ListBuffer[Job_node]()
    val edges = job_node.out_edges

    if (edges.length > 0)
      for (e <- edges) {
        tmpnodes.append(e.to_node)
      }

    return tmpnodes
  }

  def all_nodes_done: Boolean = {
    for (v <- nodes.values) {
      if (v.status != NodeStatus.Finished) {
        return false
      }
    }
    return true
  }

  def all_previous_done_nodes(job_node: Job_node): Boolean = {
    val in_edges = job_node.in_edges
    if (in_edges.length < 1)
      return false;

    for (e <- in_edges) {
      if (e.from_node.status == NodeStatus.None || e.from_node.status == NodeStatus.Started)
        return false
    }
    return true;
  }

  def get_node_name(node_name: String): Job_node = return nodes(node_name)

  def updateNodeStatus(job_node: Job_node, nodeStatus: NodeStatus.Value): Unit = {
    job_node.status = nodeStatus;
  }


  def get_job_node_by_name(node_name: String): Job_node = {
    var job_node: Job_node = null;
    for (v <- nodes.values) {
      if (v.npath == node_name)
        job_node = v
    }
    (job_node)

  }

  def get_siblling_nodes(node_name: String): Boolean = {
    println("\n\rsibllings for " + node_name)

    var job_node: Job_node = null;
    for (v <- nodes.values) {
      if (v.npath == node_name)
        job_node = v
    }

    if (job_node == null)
      println("Node not found ")


    val in_edges = job_node.in_edges
    if (in_edges.length < 1)
      return false;

    var tmp_nodes: ListBuffer[Job_node] = null
    for (e <- in_edges) {
      val parent_node = e.from_node
      println(s"{${parent_node.id} ----------->" + parent_node.npath)
      for (out_edge <- parent_node.out_edges) {
        println(s"(${parent_node.id})" + out_edge.to_node.id + "-" + out_edge.to_node.npath)
      }

    }

    return true;
  }

  def get_all_parents_nodes(node_name: String): Unit = {


    var job_node: Job_node = null;
    for (v <- nodes.values) {
      if (v.npath == node_name)
        job_node = v
    }
    println("Start Node " + job_node.id + " -- " + job_node.npath)
    get_all_parents_nodes_rec(job_node, "->")
  }

  def get_all_parents_nodes_rec(job_node: Job_node, space: String): Boolean = {
    val in_edges = job_node.in_edges
    if (in_edges.length < 1)
      return false;

    var tmp_nodes: ListBuffer[Job_node] = null
    for (e <- in_edges) {
      val parent_node = e.from_node
      println(s"{${parent_node.id} " + space + parent_node.npath)
      get_all_parents_nodes_rec(parent_node, "--" + space)
    }

    return true;
  }

  def print_graph(node: Job_node, printed_nodes: mutable.Set[String]): Unit = {
    var tmp_nodes: ListBuffer[Job_node] = null
    if (node == null)
      tmp_nodes = get_level1_nodes
    else
      tmp_nodes = get_next_nodes(node)

    if (tmp_nodes == null || tmp_nodes.length < 0)
      return;

    for (n <- tmp_nodes) {
      if (printed_nodes == null) {
        printed_nodes += n.key
        println(n.key)
      } else if (printed_nodes.contains(n.key) == false) {
        println(n.key)
        printed_nodes += n.key
      }
    }

    for (n <- tmp_nodes)
      print_graph(n, printed_nodes)
  }

}


class Job_node(name: String) {
  var out_edges = new ListBuffer[JobEdge]
  var in_edges = new ListBuffer[JobEdge]
  var status = NodeStatus.None
  var start_time: Date = null
  var end_time: Date = null
  var key = name;
  var dummyNode = false
  var properties: Map[String, String] = null
  var data: String = null
  var nType: String = null
  var npath: String = null
  var id: Int = 0


  def this(node_name: String, nPath: String, nType: String, node_data: String) {
    this(node_name)
    this.nType = nType
    this.npath = nPath
    this.data = node_data
  }


  def update_status(new_status: NodeStatus.Value) {
    status = new_status
  }


}

case class JobEdge(edge_id: String, from_node: Job_node, to_node: Job_node)

class GraphService {

  def loadGraphData(linkfilename: String, executionParam: ExecutionParam): Graph = {

    import scala.io.Source

    val runForOrgans = executionParam.runForOrgans
    var header = true
    val graph = new Graph()
    for (line <- Source.fromFile(linkfilename).getLines()) {

      if (header) {
        header = false
        print(line)
      } else {

        var add: Boolean = true
        //        if (runForOrgans.length > 0) {
        //          for (orgn <- runForOrgans) {
        //            if (line.contains(orgn) || line.contains("null"))
        //              add = true
        //          }
        //        } else {
        //          add = true
        //        }


        if (add) {
          var parentNode: Job_node = null;
          var childNode: Job_node = null;

          val splits = line.split(",")
          if (splits.length < 6)
            println(line)

          val parentId: String = splits(0)
          val parentnPath: String = splits(1)
          val parentType: String = splits(2)


          if (parentId != "null") {

            var npath: String = null
            if (parentnPath != "null")
              npath = parentnPath.split("/")(1)

            val parentNodeKey = parentId + "-" + npath;

            parentNode = graph.get_node(parentNodeKey)

            if (parentNode == null) {
              //              parentNode = new Job_node(node_name=parentId, npath = npath, nType = parentType,node_data = "")
              parentNode = new Job_node(parentNodeKey, npath, parentType, "")
              parentNode.id = parentId.toInt
              graph.add_node(parentNode)
            }
          }

          val childnPath = splits(3)
          val childType: String = splits(4)
          val childId: String = splits(5)


          if (childId != "null") {
            var npath: String = null
            if (childnPath != "null")
              npath = childnPath.split("/")(1)

            val childNodeKey = childId + "-" + npath;

            childNode = graph.get_node(childNodeKey)
            if (childNode == null) {
              childNode = new Job_node(childNodeKey, npath, childType, "")
              childNode.id = childId.toInt
              graph.add_node(childNode)
            }
          }

          if (parentNode != null && childNode != null)
            graph.add_Edge(parentNode, childNode)

        }
      }
    }
    return graph;

  }


  def executeGraph(graph: Graph, sc: SparkContext, poolSize: Int, spark: SparkSession, batchDetails: BatchDetails, executionParam: ExecutionParam, caplogger: AppLogger): Int = {

    val dataExtractionService = new DataExtractionService()
    //400
    val taskMaster = new TaskMaster(executionParam.parallelThread, graph)
    var index = 0;


    try {

      var level = 0
      var nodes: ListBuffer[Job_node] = graph.get_nodes_for_execution
      println("Total Nodes : " + nodes.size)
      new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("GRAPH_INFO").withGroup("LEVEL").withRemark(level + "").withRemarkType(nodes.size + "").buildAndLog(caplogger);

      val totalNodes = nodes.size


      var keepgoing = true

      val runForOrgans: List[String] = executionParam.runForOrgans
      while (nodes.length > 0 || keepgoing == true) {

        keepgoing = false;
        if (nodes.length > 0) {
          level = level + 1
          new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("GRAPH_INFO").withGroup("LEVEL").withRemark(level + "").withRemarkType(nodes.size + "").buildAndLog(caplogger)
        };
        //29000
        for (n <- nodes) {
          val key = n.key

          n.status = NodeStatus.init
          new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(n.key).withGroup("TASK_DETAILS").withType("TASK_BEFORE_SUBMITTED").buildAndLog(caplogger);
          //          val capTestTask = new SimpleSparkTestTask(spark = spark, sc = sc, job_node = n, executionParam = executionParam, batchDetails = batchDetails,caplogger=caplogger)
          val capTestTask = new CapTask(spark = spark, sc = sc, job_node = n, executionParam = executionParam, batchDetails = batchDetails, caplogger = caplogger)

          index = index + 1
          capTestTask.index = index

          taskMaster.submitTask(capTestTask)

          //          println(s"Node # $index of $totalNodes nodes submitted ")
          //        capTestTask.call()

        }


        nodes = graph.get_nodes_for_execution
        if (nodes.size == 0 && graph.all_nodes_done == false) {
          keepgoing = true
          taskMaster.releaseTokens()
        };

      }

    } catch {
      case e: Runtime => throw new RuntimeException(e)
      case e: Throwable => throw new RuntimeException(e)

    } finally {
      taskMaster.shutItDownNow()
    }
    return index;

  }

  def executeTestGraph(graph: Graph, sc: SparkContext, poolSize: Int, spark: SparkSession, batchDetails: BatchDetails, executionParam: ExecutionParam, caplogger: AppLogger): Int = {

    val dataExtractionService = new DataExtractionService()
    //400
    val taskMaster = new TaskMaster(executionParam.parallelThread, graph)
    var index = 0;


    try {

      var level = 0
      var nodes: ListBuffer[Job_node] = graph.get_nodes_for_execution
      println("Total Nodes : " + nodes.size)
      new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("GRAPH_INFO").withGroup("LEVEL").withRemark(level + "").withRemarkType(nodes.size + "").buildAndLog(caplogger);

      val totalNodes = nodes.size


      var keepgoing = true

      val runForOrgans: List[String] = executionParam.runForOrgans
      while (nodes.length > 0 || keepgoing == true) {

        keepgoing = false;
        if (nodes.length > 0) {
          level = level + 1
          new LogEntryBuilder().withBatchId(batchDetails.batch_id).withType("GRAPH_INFO").withGroup("LEVEL").withRemark(level + "").withRemarkType(nodes.size + "").buildAndLog(caplogger)
        };
        //29000
        for (n <- nodes) {
          val key = n.key

          n.status = NodeStatus.init
          new LogEntryBuilder().withBatchId(batchDetails.batch_id).withTaskId(n.key).withGroup("TASK_DETAILS").withType("TASK_BEFORE_SUBMITTED").buildAndLog(caplogger);
          //          val capTestTask = new SimpleSparkTestTask(spark = spark, sc = sc, job_node = n, executionParam = executionParam, batchDetails = batchDetails,caplogger=caplogger)
          val task = new SimpleSparkTestTask(spark = spark, sc = sc, job_node = n, executionParam = executionParam, batchDetails = batchDetails, caplogger = caplogger)

          index = index + 1
          task.index = index

          taskMaster.submitTask(task)

          //          println(s"Node # $index of $totalNodes nodes submitted ")
          //        capTestTask.call()

        }


        nodes = graph.get_nodes_for_execution
        if (nodes.size == 0 && graph.all_nodes_done == false) {
          keepgoing = true
          taskMaster.releaseTokens()
        };

      }

    } catch {
      case e: Runtime => throw new RuntimeException(e)
      case e: Throwable => throw new RuntimeException(e)

    } finally {
      taskMaster.shutItDownNow()

    }
    return index;

  }


}


