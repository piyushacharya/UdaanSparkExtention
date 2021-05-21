package capillary

import scala.collection.mutable.ListBuffer
import java.io._
import java.time.LocalDateTime
import org.apache.http.HttpHost
import org.codehaus.jackson.map.ObjectMapper
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{ClearScrollRequest, ClearScrollResponse, SearchRequest, SearchResponse, SearchScrollRequest}
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.metrics.ParsedMax
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders, TermQueryBuilder}
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.reindex.DeleteByQueryRequest
import org.elasticsearch.search.Scroll

import scala.collection.mutable

trait AppLogger {
  var loggerSwitch = true;

  def log(logEntry: LogEntry): Unit

  def getIncompltedTasks(batch_id: Long): mutable.ListBuffer[LogEntry]

  def getCompltedTasks(batch_id: Long): ListBuffer[String]

  def getAllTasks(batch_id: Long): ListBuffer[LogEntry]


  def purgeAllLogs(): Unit

  def cleanupInCompleteLogs(batch_id: Long): Unit

  def getLatestBathcId(): Long

  def closeLogger(): Unit

  def storeBatchDetails(batchDetails: BatchDetails, executionParam: ExecutionParam): Unit
}


class LogEntry() {
  val log = new java.util.HashMap[String, AnyRef]()

  def this(logValues: java.util.Map[String, AnyRef]) {
    this()
    logValues.keySet().forEach(x => log.put(x, logValues.get(x) + "")
    )
  }

}


class LogUtilities() {

  def groupBatchDataOnTaskLog(logs: ListBuffer[LogEntry]): mutable.HashMap[String, ListBuffer[LogEntry]] = {
    var groupedLog: mutable.HashMap[String, ListBuffer[LogEntry]] = new mutable.HashMap[String, ListBuffer[LogEntry]]()

    logs.foreach(x => {
      if (x.log.containsKey("TASK_ID")) {
        val task_id: String = x.log.get("TASK_ID").toString
        if (groupedLog.contains(task_id)) {
          groupedLog.get(task_id).get += x
        }
        else {
          var list = new ListBuffer[LogEntry]
          groupedLog.put(task_id, list)
          list += x
        }
      }
    })
    return groupedLog;
  }
}


class LogEntryBuilder {

  var batch_id: Long = 0L
  var index: Long = 0L
  var log_time: LocalDateTime = null
  var log_type: String = null
  var task_id: String = null
  var value: String = null
  var remark: String = null
  var dep: String = null

  var group: String = null
  var remark_type: String = null
  var dummy: String = null
  var query: String = null

  var spark_job_id: String = null;
  var minimal = false

  def LogEntryBuilder(): Unit = {
    this.minimal = true
  }

  def withBatchId(batch_id: Long): LogEntryBuilder = {
    this.batch_id = batch_id
    return this;
  }

  def withIndex(index: Long): LogEntryBuilder = {
    this.index = index
    return this;
  }

  def withTaskId(task_id: String): LogEntryBuilder = {
    this.task_id = task_id
    return this;
  }

  def withType(log_type: String): LogEntryBuilder = {
    this.log_type = log_type
    return this;
  }

  def withGroup(group: String): LogEntryBuilder = {
    this.group = group
    return this;
  }

  def withRemark(remark: String): LogEntryBuilder = {
    this.remark = remark
    return this;
  }

  def withDep(dep: String): LogEntryBuilder = {
    this.dep = dep
    return this;
  }

  def withRemarkType(remark_type: String): LogEntryBuilder = {
    this.remark_type = remark_type
    return this;
  }

  def withDummy(dummy: String): LogEntryBuilder = {
    this.dummy = dummy
    return this;
  }


  def withQuery(query: String): LogEntryBuilder = {
    this.query = query
    return this;
  }


  def buildAndLog(logger: AppLogger): Unit = logger.log(build())

  def build(): LogEntry = {
    this.log_time = LocalDateTime.now();

    val loga = new LogEntry()

    loga.log.put("LOG_TIME", LocalDateTime.now())

    if (batch_id > 0)
      loga.log.put("BATCH_ID", Integer.valueOf(batch_id + ""))

    if (index > 0)
      loga.log.put("INDEX", Integer.valueOf(index + ""))

    if (task_id != null)
      loga.log.put("TASK_ID", task_id + "")

    if (log_type != null)
      loga.log.put("LOG_TYPE", log_type + "")

    if (remark != null)
      loga.log.put("REMARK", remark + "")

    if (remark_type != null)
      loga.log.put("REMARK_TYPE", remark_type + "")

    if (group != null)
      loga.log.put("GROUP", group + "")

    if (dep != null)
      loga.log.put("DEP", dep + "")

    if (dummy != null)
      loga.log.put("DUMMY", dummy + "")


    if (minimal == false && query != null && query != "")
      loga.log.put("QUERY", query + "")

    return loga;
  }

}

class FileBasedLogger(val loggerBaseLocation: String) extends AppLogger {

  val file = new File(loggerBaseLocation + "/" + java.util.UUID.randomUUID.toString + "_log.txt")
  val bw = new BufferedWriter(new FileWriter(file))
  val mapper = new ObjectMapper()

  def this(location: String, loggerSwitch: Boolean) {
    this(loggerBaseLocation = location)
    this.loggerSwitch = loggerSwitch
  }

  override def log(logEntry: LogEntry): Unit = {
    if (this.loggerSwitch) {
      val res = mapper.writeValueAsString(logEntry.log)
      bw.write(res + "\n\r")
      bw.flush()
    }
  }

  override def getIncompltedTasks(batch_id: Long): mutable.ListBuffer[LogEntry] = {
    return null;
  }

  override def getCompltedTasks(batch_id: Long): ListBuffer[String] = {

    return null;
  }

  override def getAllTasks(batch_id: Long): ListBuffer[LogEntry] = {
    return null
  }


  override def purgeAllLogs(): Unit = {

  }

  override def cleanupInCompleteLogs(batch_id: Long): Unit = {

  }

  override def getLatestBathcId(): Long = {
    return 1L;
  }

  override def closeLogger(): Unit = {
    bw.close()
  }

  override def storeBatchDetails(batchDetails: BatchDetails, executionParam: ExecutionParam): Unit = ???
}

case class EsConfig(server: String, port: Int)

class EsLogIngestor(restClient: RestHighLevelClient, logs: java.util.concurrent.ConcurrentLinkedQueue[LogEntry], ingestionRate: Long) extends Thread {
  var startLog = true

  def stopLog(): Unit = {
    startLog = false
    while (logs.size() > 0) {
      Thread.sleep(2000)
    }
  }

  override def run(): Unit = {
    while (startLog || logs.size() > 0) {

      if (logs.size() > 0) {
        var index: Long = 0;
        val bulkRequest = new BulkRequest()
        var add = false
        while (index < ingestionRate) {
          val log = logs.poll()
          index = index + 1
          if (log != null) {
            add = true
            val esLogEntry = new IndexRequest("batch_log")
            //            if (log.log.get("LOG_TYPE")=="TASK_ENDED_INFO")
            //            {
            //              println("STOP!!!")
            //            }
            //            println(log.log.get("LOG_TYPE") + s" Log size [${logs.size()}] ")
            esLogEntry.source(log.log)
            bulkRequest.add(esLogEntry)
          } else {
            index = ingestionRate + 1
          }
        }
        if (logs.size() > 0) {
          println(s" Log left  [${logs.size()}] ")
        }


        if (add == true) {
          restClient.bulk(bulkRequest, RequestOptions.DEFAULT)
        }

      } else
        Thread.sleep(1000)
    }

  }
}

class EsBasedLogger() extends AppLogger {


  val batch_details_index_name = "batch_details"
  val batch_log_index_name = "batch_log"
  val logs = new java.util.concurrent.ConcurrentLinkedQueue[LogEntry]
  var ingestor: EsLogIngestor = null

  var restClient: RestHighLevelClient = null;

  //    def this(restClient: RestHighLevelClient) {
  //      this()
  //      this.restClient = restClient;
  //    }
  //
  //    def this(restClientStr: String) {
  //      this()
  //      this.restClient = new RestHighLevelClient(RestClient.builder(
  //        restClientStr))
  //    }


  def startIngestor(): Unit = {
    ingestor = new EsLogIngestor(restClient, logs, 5000)
    ingestor.start()
  }

  def this(hostName: String, port: Int) {
    this()
    restClient = new RestHighLevelClient(RestClient.builder(
      new HttpHost("localhost", 9200, "http"),
      new HttpHost("localhost", 9201, "http")));
    checkOrCreateIndex(List(batch_details_index_name, batch_log_index_name, "my-index"))
    startIngestor()
  }

  def this(primaryHostName: String, primaryPort: Int, secondaryHostName: String, secondaryPort: Int) {
    this()
    restClient = new RestHighLevelClient(RestClient.builder(
      new HttpHost(primaryHostName, primaryPort, "http"),
      new HttpHost(secondaryHostName, secondaryPort, "http")));

    checkOrCreateIndex(List(batch_details_index_name, batch_log_index_name, "my-index"))
    startIngestor()
  }


  override def log(logEntry: LogEntry): Unit = {

    if (this.loggerSwitch) {
      logs.add(logEntry)
      //      val esLogEntry = new IndexRequest(batch_log_index_name)
      //      esLogEntry.source(logEntry.log)
      //      try{
      //        val indexResponse2 = restClient.index(esLogEntry, RequestOptions.DEFAULT)
      //      }

    }
  }

  override def getIncompltedTasks(batch_id: Long): mutable.ListBuffer[LogEntry] = {
    val logs = getAllTasks(batch_id)
    val logUtilities = new LogUtilities();
    val groupBatchData = logUtilities.groupBatchDataOnTaskLog(logs);

    val inCompletedTasks = new mutable.ListBuffer[LogEntry]


    groupBatchData.foreach(x => {
      val entryLogs = x._2;
      var taskInComplete = false;
      var taskEnd: LogEntry = null;
      entryLogs.foreach(e => {
        if (e.log.containsKey("TASK_END")) {
          taskEnd = e
        }
      })

      if (taskEnd != null)
        inCompletedTasks += taskEnd

    })

    return inCompletedTasks;
  }

  def getCompltedTasks(batch_id: Long): ListBuffer[String] = {

    val logList = new ListBuffer[String]

    val scroll: Scroll = new Scroll(TimeValue.timeValueMinutes(1L));
    val searchRequest: SearchRequest = new SearchRequest("batch_log");
    searchRequest.scroll(scroll);
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(1000);
    val qb1 = QueryBuilders.termQuery("BATCH_ID", batch_id).boost(1.0f)
    val qb2 = QueryBuilders.matchQuery("LOG_TYPE", "TASK_ENDED").boost(1.0f)

    val qb3 = QueryBuilders.termQuery("LOG_TYPE", "TASK_STARTED").boost(1.0f)

    val boolQueryBuilder = new BoolQueryBuilder
    boolQueryBuilder.must.add(qb1)
    boolQueryBuilder.must.add(qb2)
    //boolQueryBuilder.should.add(qb3)

    searchSourceBuilder.query(boolQueryBuilder);

    searchRequest.source(searchSourceBuilder);

    var searchResponse: SearchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);
    var scrollId: String = searchResponse.getScrollId();
    var searchHits = searchResponse.getHits().getHits();

    while (searchHits != null && searchHits.length > 0) {


      if  (searchHits.length >0)
      {
          for (n <- searchHits)
          {
            val log = n.getSourceAsMap()
            logList += log.get("TASK_ID").toString
          }
      }

      val scrollRequest: SearchScrollRequest = new SearchScrollRequest(scrollId);
      scrollRequest.scroll(scroll);
      searchResponse = restClient.scroll(scrollRequest, RequestOptions.DEFAULT);
      scrollId = searchResponse.getScrollId();
      searchHits = searchResponse.getHits().getHits();
    }

    val clearScrollRequest:ClearScrollRequest = new ClearScrollRequest();
    clearScrollRequest.addScrollId(scrollId);
    val clearScrollResponse:ClearScrollResponse = restClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    val succeeded = clearScrollResponse.isSucceeded();


    return logList;
  }

  override def getAllTasks(batch_id: Long): ListBuffer[LogEntry] = {
    val sourceBuilder = new SearchSourceBuilder();
    val boolQueryBuilder = new BoolQueryBuilder();
    val qb1 = QueryBuilders.termQuery("BATCH_ID", batch_id);
    boolQueryBuilder.must.add(qb1);
    sourceBuilder.query(boolQueryBuilder);
    val searchRequest = new SearchRequest(batch_log_index_name); // your index name
    searchRequest.source(sourceBuilder);
    val searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);

    val hits = searchResponse.getHits()
    val totalHits = hits.getTotalHits

    val logList = new ListBuffer[LogEntry]
    if (totalHits.value > 0) {
      val itr = hits.iterator()
      while (itr.hasNext) {
        val hit = itr.next();
        val fields = hit.getSourceAsMap()

        println(fields)
        val log = new LogEntry(fields)
        logList += log
      }
    }

    return logList
  }


  override def purgeAllLogs(): Unit = {

    val list = List(batch_details_index_name, batch_log_index_name)

    list.foreach(x => {
      val request = new DeleteIndexRequest(x)
      import org.elasticsearch.client.RequestOptions
      val deleteIndexResponse = restClient.indices.delete(request, RequestOptions.DEFAULT)
    })

    checkOrCreateIndex(list)

  }

  override def cleanupInCompleteLogs(batch_id: Long): Unit = {
    val request =
      new DeleteByQueryRequest(batch_details_index_name, batch_log_index_name);
    request.setQuery(new TermQueryBuilder("batch_id", batch_id));
    val bulkResponse = restClient.deleteByQuery(request, RequestOptions.DEFAULT);

  }


  override def storeBatchDetails(batchDetails: BatchDetails, executionParam: ExecutionParam): Unit = {

    val log = new java.util.HashMap[String, AnyRef]()
    log.put("batch_id", Integer.valueOf(batchDetails.batch_id + ""))
    log.put("restart", batchDetails.restart + "")
    log.put("node_folder_loc", batchDetails.node_folder_loc + "")
    log.put("spark_mode", batchDetails.spark_mode + "")
    log.put("node_link_loc", batchDetails.node_link_loc + "")
    log.put("seed_file_loc", batchDetails.seed_file_loc + "")
    log.put("seed_rel_file_loc", batchDetails.seed_rel_file_loc + "")
    log.put("parallelThread", executionParam.parallelThread + "")


    val esLogEntry = new IndexRequest(batch_details_index_name)
    esLogEntry.source(log)
    val indexResponse2 = restClient.index(esLogEntry, RequestOptions.DEFAULT)
  }


  override def getLatestBathcId(): Long = {

    val searchRequestmax = new SearchRequest(batch_details_index_name);

    val aggregation =
      AggregationBuilders
        .max("batch_id_max_value")
        .field("batch_id");

    val searchSourceBuilderMax = new SearchSourceBuilder();
    searchSourceBuilderMax.aggregation(aggregation);
    searchSourceBuilderMax.size(0)

    searchRequestmax.source(searchSourceBuilderMax);
    val searchResponse2 = restClient.search(searchRequestmax, RequestOptions.DEFAULT)


    println(searchResponse2)
    val agg = searchResponse2.getAggregations

    var value = null

    if (agg.asMap().containsKey("batch_id_max_value")) {
      val a: ParsedMax = agg.asMap().get("batch_id_max_value").asInstanceOf[ParsedMax]
      if (a.getValue().toLong < 0)
        return 0
      return a.getValue().toLong
    }
    return 0;
  }

  override def closeLogger(): Unit = {
    ingestor.stopLog()
    restClient.close()


  }


  def checkOrCreateIndex(indexNames: List[String]): Unit = {


    for (indexname <- indexNames) {
      val request = new GetIndexRequest(
        indexname);
      //      getRequest.fetchSourceContext(new FetchSourceContext(false));
      //      getRequest.storedFields("_none_");
      request.local(false);
      request.humanReadable(true);
      request.includeDefaults(false);


      val exists = restClient.indices().exists(request, RequestOptions.DEFAULT);

      //      val exists = restClient.exists(getRequest, RequestOptions.DEFAULT)

      println(s"Index found [$indexname] [$exists]")

      if (exists == false) {
        val request = new CreateIndexRequest(indexname);
        request.settings(Settings.builder()
          .put("index.number_of_shards", 3)
          .put("index.number_of_replicas", 2)
        );

        val createIndexResponse = restClient.indices().create(request, RequestOptions.DEFAULT);
        println(s"Index created [$indexname] ")
      }
    }


  }

}

