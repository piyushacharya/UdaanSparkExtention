package capillary

import java.util.concurrent.{Callable, ConcurrentHashMap, Executors, ScheduledFuture, TimeUnit}
import scala.collection.mutable.ListBuffer
import scala.util.Random

trait BaseTask extends Callable[String] {

  def taskStatus

  def getToken

  def setToken(token: String): Unit

  def getResult

  def setFuture(future: ScheduledFuture[String]): Unit

  def cancelJob(): Unit

  def isLongJob: Boolean
}

abstract class GenericTask() extends BaseTask {

  var status = TaskStatus.Init;
  var future: ScheduledFuture[String] = null
  var longJob = false
  var token: String = null
  var index = 0
  var errorMessage: String = null;

  override def taskStatus = status

  override def getToken = this.token

  override def setToken(token: String): Unit = this.token = token

  override def getResult = this.future.get()

  override def setFuture(future: ScheduledFuture[String]): Unit = this.future = future

  override def cancelJob(): Unit = {
    status = TaskStatus.CalledForCancel
    this.future.cancel(true)
  }

  override def isLongJob = this.longJob
}


object TaskStatus extends Enumeration {
  val Init, None, Started, Finished, Error, CalledForCancel = Value
}

class TaskMaster(poolSize: Int, graph: Graph) {

  private val executor = Executors.newScheduledThreadPool(poolSize)
  private var runningTasks: ConcurrentHashMap[String, GenericTask] = new ConcurrentHashMap()
  private val tokens: ListBuffer[String] = genTokens(poolSize)
  var status = "ACTIVE"
  var status_error_message =""

  private def genTokens(count: Int): ListBuffer[String] = {
    val tokens = new ListBuffer[String];
    for (n <- 1 to count) {
      tokens += "token-" + n
    }
    return tokens;
  }

  def releaseTokens(): Int = {
    val completedTokens = new ListBuffer[String]()
    val itr = runningTasks.values().iterator()
    while (itr.hasNext) {
      val tsk = itr.next()

      //&& tsk.status != TaskStatus.CalledForCancel
      if (tsk.isLongJob) {
        System.err.println("Task called for cancel #  Token [ " + tsk.token + " ]" + "Task status [ " + tsk.taskStatus + " ]")
        tsk.cancelJob()
      }
      if (tsk.status == TaskStatus.Error) {
        if (graph != null)
          graph.status = "Inactive"
        completedTokens += tsk.token
        status = "ERROR"
        status_error_message = tsk.errorMessage
        throw new RuntimeException("Error occurred during task index [ " + tsk.index + " ] name [ " + tsk.token+ " ]\n\r" + tsk.errorMessage)

      } else if (tsk.status == TaskStatus.Finished || tsk.taskStatus == TaskStatus.Error || (tsk.status == TaskStatus.CalledForCancel && tsk.future.isDone)) {
        //println(s"Node # " + tsk.index + " Finished")
        completedTokens += tsk.token
      }
    }

    if (completedTokens.size > 0) {
//      println("Tokens released " + completedTokens)
      for (t <- completedTokens) {
        runningTasks.remove(t)
        tokens += t
      }
    }
    return completedTokens.size
  }

  def submitTask(task: GenericTask): Unit = {


    var jobSubmitFail = false

    while (!jobSubmitFail) {

      if (tokens.size > 0) {
        val token = tokens.remove(0)
        task.setToken(token)
        val result: ScheduledFuture[String] = executor.schedule(task, 0, TimeUnit.MILLISECONDS)
        task.setFuture(result)
        runningTasks.put(task.token, task)
        jobSubmitFail = true
      } else {

        val tokenReleaseCount = releaseTokens()
        if (tokenReleaseCount <= 0) {
          Thread.sleep(500)

        }
      }
    }
  }

  def releaseUntilItsDone(): Unit = {
    while (runningTasks.size() > 0) {
      Thread.sleep(500)
      releaseTokens()
    }
  }

  def shutItDown(): Unit = {
    releaseUntilItsDone();
    executor.shutdown()
  }

  def shutItDownNow(): Unit = executor.shutdown()

  def allTaskAreDone(): Boolean = !(runningTasks.size() > 0)
}

