/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.{ExecutorAllocationClient, Logging, SparkEnv, SparkException, TaskState}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ActorLogReceive, SerializableBuffer, AkkaUtils, Utils}

/**
  * A scheduler backend that waits for coarse grained executors to connect to it through Akka.
  * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
  * executors whenever a task is done and asking the scheduler to launch a new executor for
  * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
  * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
  * (spark.deploy.*).
  */
/*
*
*CoarseGrainedSchedulerBackend是属于Driver端的backend，在该class中包含着一个DriverActor，用来通信，定义了DriverActor的实现；接受executor发来的信号，同时接受来自DAGScheduler端的信号
*
* */
private[spark]
class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val actorSystem: ActorSystem)
        extends ExecutorAllocationClient with SchedulerBackend with Logging {
    // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
    var totalCoreCount = new AtomicInteger(0)
    // Total number of executors that are currently registered
    var totalRegisteredExecutors = new AtomicInteger(0)
    val conf = scheduler.sc.conf
    private val timeout = AkkaUtils.askTimeout(conf)
    private val akkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)
    // Submit tasks only after (registered resources / total expected resources)
    // is equal to at least this value, that is double between 0 and 1.
    var minRegisteredRatio =
    math.min(1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0))
    // Submit tasks after maxRegisteredWaitingTime milliseconds
    // if minRegisteredRatio has not yet been reached
    val maxRegisteredWaitingTime =
    conf.getInt("spark.scheduler.maxRegisteredResourcesWaitingTime", 30000)
    val createTime = System.currentTimeMillis()

    /*
    *
    * 该数据结构及其重要，保存着所有的executor的信息，注册信息都在里面
    * ExecutorData保存着executor的ip、executoractor、core
    * */
    private val executorDataMap = new HashMap[String, ExecutorData]

    // Number of executors requested from the cluster manager that have not registered yet
    private var numPendingExecutors = 0

    private val listenerBus = scheduler.sc.listenerBus

    // Executors we have requested the cluster manager to kill that have not died yet
    private val executorsPendingToRemove = new HashSet[String]

    //  Actor主要是两个函数：preStart，receiveWithLogging
    class DriverActor(sparkProperties: Seq[(String, String)]) extends Actor with ActorLogReceive {
        override protected def log = CoarseGrainedSchedulerBackend.this.log

        private val addressToExecutorId = new HashMap[Address, String]

        //  //根据akka的特性，我们知道preStart方法会在actor的构造函数执行完毕后，执行
        override def preStart() {
            // Listen for remote client disconnection events, since they don't go through Akka's watch()
            context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

            // Periodically revive offers to allow delay scheduling to work
            val reviveInterval = conf.getLong("spark.scheduler.revive.interval", 1000)
            import context.dispatcher
            //  向自己this，即DriverActor周期性的发送消息ReviveOffers，0秒后，每隔reviveIterval.millis发送一次消息
            //  周期性的执行启动所有task的任务
            //  还有一个执行ReviveOffers的地方，是TaskSchedulerImpl.submitTasks(taskSet: TaskSet)中调用backend.reviveOffers()
            context.system.scheduler.schedule(0.millis, reviveInterval.millis, self, ReviveOffers)
        }

        /*
        *
        * DriverActor主要接收几种message：executor相关的Register stop remove Executor、task相关的ReviveOffers、killtask等
        * */
        def receiveWithLogging = {
            //  收到CoarseGrainedExecutorBackend的注册消息，将该新executor的信息保存在executorDataMap中
            case RegisterExecutor(executorId, hostPort, cores, logUrls) =>
                Utils.checkHostPort(hostPort, "Host port expected " + hostPort)
                //  不能重复注册，其中sender是发送消息方
                if (executorDataMap.contains(executorId)) {
                    sender ! RegisterExecutorFailed("Duplicate executor ID: " + executorId)
                } else {
                    //  如果是新的executor
                    logInfo("Registered executor: " + sender + " with ID " + executorId)
                    /*
                    *
                    * BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*
                    * BUG* 感觉这里是个bug，如果addressToExecutorId(sender.path.address) = executorId没有成功的话，但是已经向CoarseGrainedExecutorBackend发送了“注册成功”的消息
                    * BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*BUG*
                    * */
                    sender ! RegisteredExecutor

                    addressToExecutorId(sender.path.address) = executorId
                    totalCoreCount.addAndGet(cores)
                    totalRegisteredExecutors.addAndGet(1)
                    val (host, _) = Utils.parseHostPort(hostPort)
                    //  这是第一次ExecutorData实例化，发生在CoarseGrainedExecutorBackend向CoarseGrainedSchedulerBackend的DriverActor注册时候
                    val data = new ExecutorData(sender, sender.path.address, host, cores, cores, logUrls)
                    // This must be synchronized because variables mutated
                    // in this block are read when requesting executors

                    //  ************** 在这里之所以用同步锁synchronized，是因为有很多CoarseGrainedExecutorBackend注册，可能会出现同时读写的问题
                    CoarseGrainedSchedulerBackend.this.synchronized {
                        executorDataMap.put(executorId, data)
                        if (numPendingExecutors > 0) {
                            numPendingExecutors -= 1
                            logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
                        }
                    }
                    //  向总线添加监听器，这一块不是很清楚。。。。。。。总线源码需要认真研读一下
                    listenerBus.post(
                        SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))

                    //  这里也调用了makeOffers，启动当前对应的TaskSchedulerImpl的所有的task
                    //  为什么要这样做呢？因为现在刚刚注册一个新的executor，我们需要将tasks重新分配给所有可用executor中，task的资源分配情况发生了变化
                    makeOffers()
                }

            case StatusUpdate(executorId, taskId, state, data) =>
                scheduler.statusUpdate(taskId, state, data.value)
                //  如果收到task的计算结果TaskState，发现状态state为已完成，则将ExecutorDataMap中对应executor的信息修改，运行完task后，释放了该task占用的core，可用executor资源ExecutorDataMap要增加
                if (TaskState.isFinished(state)) {
                    executorDataMap.get(executorId) match {
                        case Some(executorInfo) =>
                            executorInfo.freeCores += scheduler.CPUS_PER_TASK
                            makeOffers(executorId)
                        case None =>
                            // Ignoring the update since we don't know about the executor.
                            logWarning(s"Ignored task status update ($taskId state $state) " +
                                    "from unknown executor $sender with ID $executorId")
                    }
                }

            case ReviveOffers =>
                makeOffers()

            case KillTask(taskId, executorId, interruptThread) =>
                executorDataMap.get(executorId) match {
                    case Some(executorInfo) =>
                        executorInfo.executorActor ! KillTask(taskId, executorId, interruptThread)
                    case None =>
                        // Ignoring the task kill since the executor is not registered.
                        logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
                }

            case StopDriver =>
                sender ! true
                context.stop(self)

            case StopExecutors =>
                logInfo("Asking each executor to shut down")
                for ((_, executorData) <- executorDataMap) {
                    executorData.executorActor ! StopExecutor
                }
                sender ! true

            case RemoveExecutor(executorId, reason) =>
                removeExecutor(executorId, reason)
                sender ! true

            case DisassociatedEvent(_, address, _) =>
                addressToExecutorId.get(address).foreach(removeExecutor(_,
                    "remote Akka client disassociated"))

            case RetrieveSparkProps =>
                sender ! sparkProperties
        }

        // Make fake resource offers on all executors
        /*
        * executorDataMap：代码中的executorDataMap，在客户端向Master注册Application的时候，Master已经为Application分配并启动好Executor，然后注册给CoarseGrainedSchedulerBackend，注册信息就是存储在executorDataMap数据结构中。
        * TaskSchedulerImpl.resourceOffers基于这些计算资源executorDataMap为task分配Executor
        *
        * executorData和WorkerOffer是一一对应的
        * */
        //  makeOffers进行资源的调度，同时在CoarseGrainedSchedulerBackend里维护这executor的状态map:executorDataMap
        def makeOffers() {
            //  resourceOffers对所有的可用的executor资源WorkerOffer进行操作，该函数来进行task的资源分配到executor中
            //  也就是返回一个Seq[Seq[TaskDescription]]变量，保存着每个executor上task和executor的对应关系
            //  该CoarseGrainedSchedulerBackend从自己对应的TaskSchedulerImpl中拿到TaskSchedulerImpl的当前schedulableBuilder的rootPool的所有TaskSet
            launchTasks(scheduler.resourceOffers(executorDataMap.map { case (id, executorData) =>
                new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
            }.toSeq))
        }

        // Make fake resource offers on just one executor
        def makeOffers(executorId: String) {
            val executorData = executorDataMap(executorId)
            launchTasks(scheduler.resourceOffers(
                Seq(new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))))
        }

        // Launch tasks returned by a set of resource offers
        def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
            /*
            *
            * launchTasks对每个task，调用一次LaunchTask发送给executorActor
            * */
            for (task <- tasks.flatten) {
                //  task序列化
                val ser = SparkEnv.get.closureSerializer.newInstance()
                /////////////////////////////////////////////////////////////////////////***************很重要****************//////////////////////////////////////////////////////////////////////////////
                ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                //  这个serializedTask是序列化后的一个task的TaskDescription，而不是序列化后的的Task，而TaskDescription中的最后一个序列化task是在TaskSet.resourceOffer过程中，就把jar、file、task序列化了 ///
                //  总结：TaskSet.resourceOffer----Task、jar、file序列化_serializedTask，封装成TaskDescription-------在此序列化TaskDescription成serializedTask                                           ///
                //  所以CoarseGrainedExecutorBackend收到LaunchTask(new SerializableBuffer(serializedTask))后-----反序列化获得TaskDescription类型的taskDesc                                               ///
                //  ----把taskDesc的最后一个参数，即序列化后的Task交到executor上，executor.launchTask(taskId, taskDesc.serializedTask)                                                                   ///
                //  --------executor收到序列化后的Task即_serializedTask----------先反序列化获得Task、jar、file---------run函数下载jar、file，然后运行该Task                                              ///
                ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                val serializedTask = ser.serialize(task)
                if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
                    val taskSetId = scheduler.taskIdToTaskSetId(task.taskId)
                    scheduler.activeTaskSets.get(taskSetId).foreach { taskSet =>
                        try {
                            var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                                    "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
                                    "spark.akka.frameSize or using broadcast variables for large values."
                            msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize,
                                AkkaUtils.reservedSizeBytes)
                            taskSet.abort(msg)
                        } catch {
                            case e: Exception => logError("Exception in error callback", e)
                        }
                    }
                }
                else {
                    val executorData = executorDataMap(task.executorId)
                    //  对该task运行，需要占用CUPS_PER_TASK个core，对应executor的executorData的资源数量需要减去
                    executorData.freeCores -= scheduler.CPUS_PER_TASK
                    //  向executor的actor（这里说的executor就是CoarseGrainedExecutorBackend，该CoarseGrainedExecutorBackend本身就继承自Actor）发送启动序列化的task的消息
                    executorData.executorActor ! LaunchTask(new SerializableBuffer(serializedTask))
                }
            }
        }

        // Remove a disconnected slave from the cluster
        def removeExecutor(executorId: String, reason: String): Unit = {
            executorDataMap.get(executorId) match {
                case Some(executorInfo) =>
                    // This must be synchronized because variables mutated
                    // in this block are read when requesting executors
                    CoarseGrainedSchedulerBackend.this.synchronized {
                        addressToExecutorId -= executorInfo.executorAddress
                        executorDataMap -= executorId
                        executorsPendingToRemove -= executorId
                    }
                    totalCoreCount.addAndGet(-executorInfo.totalCores)
                    totalRegisteredExecutors.addAndGet(-1)
                    scheduler.executorLost(executorId, SlaveLost(reason))
                    listenerBus.post(
                        SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason))
                case None => logError(s"Asked to remove non-existent executor $executorId")
            }
        }
    }

    /////////////////////////////////////////分割线：上面的DriverActor模块结束/////////////////////////////////////////////////////
    var driverActor: ActorRef = null
    val taskIdsOnSlave = new HashMap[String, HashSet[String]]

    /*
    *
    * CoarseGrainedSchedulerBackend的start方法就是启动DriverActor进行通信，该DriverActor就在该class类中定义
    * 该start()是在SparkContext初始化过程中TaskSchedulerImpl.start()启动时，调用backend.start()启动该Actor的，SparkContext.scala的561行
    * */
    override def start() {
        val properties = new ArrayBuffer[(String, String)]
        for ((key, value) <- scheduler.sc.conf.getAll) {
            if (key.startsWith("spark.")) {
                properties += ((key, value))
            }
        }
        // TODO (prashant) send conf instead of properties
        driverActor = actorSystem.actorOf(
            Props(new DriverActor(properties)), name = CoarseGrainedSchedulerBackend.ACTOR_NAME)
    }

    //  关闭executor
    def stopExecutors() {
        try {
            if (driverActor != null) {
                logInfo("Shutting down all executors")
                val future = driverActor.ask(StopExecutors)(timeout)
                Await.ready(future, timeout)
            }
        } catch {
            case e: Exception =>
                throw new SparkException("Error asking standalone scheduler to shut down executors", e)
        }
    }

    override def stop() {
        //  向自身的driverActor发送关闭所有executor的消息
        stopExecutors()
        try {
            if (driverActor != null) {
                //  向自身的driverActor发送关闭driver的消息
                val future = driverActor.ask(StopDriver)(timeout)
                Await.ready(future, timeout)
            }
        } catch {
            case e: Exception =>
                throw new SparkException("Error stopping standalone scheduler's driver actor", e)
        }
    }

    override def reviveOffers() {
        /*
        *
        * 在TaskSchedulerImpl中的submitTasks由backend.reviveOffers()调用
        * ReviveOffers是本CoarseGrainedSchedulerBackend发送给本CoarseGrainedSchedulerBackend的
        * */
        //  消息都是大驼峰命名，函数都是小驼峰命名
        driverActor ! ReviveOffers
    }

    override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
        driverActor ! KillTask(taskId, executorId, interruptThread)
    }

    override def defaultParallelism(): Int = {
        conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
    }

    // Called by subclasses when notified of a lost worker
    def removeExecutor(executorId: String, reason: String) {
        try {
            val future = driverActor.ask(RemoveExecutor(executorId, reason))(timeout)
            Await.ready(future, timeout)
        } catch {
            case e: Exception =>
                throw new SparkException("Error notifying standalone scheduler's driver actor", e)
        }
    }

    def sufficientResourcesRegistered(): Boolean = true

    override def isReady(): Boolean = {
        if (sufficientResourcesRegistered) {
            logInfo("SchedulerBackend is ready for scheduling beginning after " +
                    s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
            return true
        }
        if ((System.currentTimeMillis() - createTime) >= maxRegisteredWaitingTime) {
            logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
                    s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTime(ms)")
            return true
        }
        false
    }





    /////////////////////////////////////以下这些函数，都是对Executor的操作（申请添加、删除、stop等）///////////////////////////////////////////////////




    /**
      * Return the number of executors currently registered with this backend.
      */
    def numExistingExecutors: Int = executorDataMap.size

    /**
      * Request an additional number of executors from the cluster manager.
      * 向集群CM请求额外数量的executor
      * @return whether the request is acknowledged.
      */
    final override def requestExecutors(numAdditionalExecutors: Int): Boolean = synchronized {
        if (numAdditionalExecutors < 0) {
            throw new IllegalArgumentException(
                "Attempted to request a negative number of additional executor(s) " +
                        s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
        }
        logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")
        logDebug(s"Number of pending executors is now $numPendingExecutors")
        numPendingExecutors += numAdditionalExecutors
        // Account for executors pending to be added or removed
        val newTotal = numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size
        doRequestTotalExecutors(newTotal)
    }

    /**
      * Express a preference to the cluster manager for a given total number of executors. This can
      * result in canceling pending requests or filing additional requests.
      *
      * @return whether the request is acknowledged.
      */
    final override def requestTotalExecutors(numExecutors: Int): Boolean = synchronized {
        if (numExecutors < 0) {
            throw new IllegalArgumentException(
                "Attempted to request a negative number of executor(s) " +
                        s"$numExecutors from the cluster manager. Please specify a positive number!")
        }
        numPendingExecutors =
                math.max(numExecutors - numExistingExecutors + executorsPendingToRemove.size, 0)
        doRequestTotalExecutors(numExecutors)
    }

    /**
      * Request executors from the cluster manager by specifying the total number desired,
      * including existing pending and running executors.
      *
      * The semantics here guarantee that we do not over-allocate executors for this application,
      * since a later request overrides the value of any prior request. The alternative interface
      * of requesting a delta of executors risks double counting new executors when there are
      * insufficient resources to satisfy the first request. We make the assumption here that the
      * cluster manager will eventually fulfill all requests when resources free up.
      *
      * @return whether the request is acknowledged.
      */
    protected def doRequestTotalExecutors(requestedTotal: Int): Boolean = false

    /**
      * Request that the cluster manager kill the specified executors.
      * Return whether the kill request is acknowledged.
      * 请求CM杀死某些executor
      */
    final override def killExecutors(executorIds: Seq[String]): Boolean = synchronized {
        logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")
        val filteredExecutorIds = new ArrayBuffer[String]
        executorIds.foreach { id =>
            if (executorDataMap.contains(id)) {
                filteredExecutorIds += id
            } else {
                logWarning(s"Executor to kill $id does not exist!")
            }
        }
        // Killing executors means effectively that we want less executors than before, so also update
        // the target number of executors to avoid having the backend allocate new ones.
        val newTotal = (numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size
                - filteredExecutorIds.size)
        doRequestTotalExecutors(newTotal)

        executorsPendingToRemove ++= filteredExecutorIds
        doKillExecutors(filteredExecutorIds)
    }

    /**
      * Kill the given list of executors through the cluster manager.
      * Return whether the kill request is acknowledged.
      */
    protected def doKillExecutors(executorIds: Seq[String]): Boolean = false

}

private[spark] object CoarseGrainedSchedulerBackend {
    val ACTOR_NAME = "CoarseGrainedScheduler"
}
