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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.{TimerTask, Timer}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.language.postfixOps
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.util.Utils
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId

/**
  * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
  * It can also work with a local setup by using a LocalBackend and setting isLocal to true.
  * It handles common logic, like determining a scheduling order across jobs, waking up to launch
  * speculative tasks, etc.
  * 启动推测任务
  *
  * Clients should first call initialize() and start(), then submit task sets through the
  * runTasks method.
  *
  * THREADING: SchedulerBackends and task-submitting clients can call this class from multiple
  * threads, so it needs locks in public API methods to maintain its state. In addition, some
  * SchedulerBackends synchronize on themselves when they want to send events here, and then
  * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
  * we are holding a lock on ourselves.
  */

/*
*
* TaskSchedulerImpl内部，基本没有任何自动运行的代码，都是一些函数定义，意味着，该TaskScheduler的所有工作，都是通过调用backend实现的
* new一个新的TaskScheduler时，不会产生任何实质性的动作，只有定义函数。。。。
* */
private[spark] class TaskSchedulerImpl(
                                              val sc: SparkContext,
                                              val maxTaskFailures: Int,
                                              isLocal: Boolean = false)
        extends TaskScheduler with Logging {
    def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

    val conf = sc.conf

    // How often to check for speculative tasks
    val SPECULATION_INTERVAL = conf.getLong("spark.speculation.interval", 100)

    // Threshold above which we warn user initial TaskSet may be starved
    val STARVATION_TIMEOUT = conf.getLong("spark.starvation.timeout", 15000)

    // CPUs to request per task
    val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

    // TaskSetManagers are not thread safe, so any access to one should be synchronized
    // on this class.
    val activeTaskSets = new HashMap[String, TaskSetManager]

    val taskIdToTaskSetId = new HashMap[Long, String]
    val taskIdToExecutorId = new HashMap[Long, String]

    @volatile private var hasReceivedTask = false
    @volatile private var hasLaunchedTask = false
    private val starvationTimer = new Timer(true)

    // Incrementing task IDs
    /*
    * 从0开始计数
    * */
    val nextTaskId = new AtomicLong(0)

    // Which executor IDs we have executors on
    val activeExecutorIds = new HashSet[String]

    // The set of executors we have on each host; this is used to compute hostsAlive, which
    // in turn is used to decide when we can attain data locality on a given host
    protected val executorsByHost = new HashMap[String, HashSet[String]]

    protected val hostsByRack = new HashMap[String, HashSet[String]]

    protected val executorIdToHost = new HashMap[String, String]

    // Listener object to pass upcalls into
    /*
    *
    * 用到了四个东西：1、DAGScheduler；2、SchedulerBackend；3、mapOutpTracker；4、SchedulebleBuilder
    *
    * */
    var dagScheduler: DAGScheduler = null

    var backend: SchedulerBackend = null

    val mapOutputTracker = SparkEnv.get.mapOutputTracker

    var schedulableBuilder: SchedulableBuilder = null
    var rootPool: Pool = null
    // default scheduler is FIFO
    private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
    val schedulingMode: SchedulingMode = try {
        SchedulingMode.withName(schedulingModeConf.toUpperCase)
    } catch {
        case e: java.util.NoSuchElementException =>
            throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
    }



    // This is a var so that we can reset it for testing purposes.
    private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

    override def setDAGScheduler(dagScheduler: DAGScheduler) {
        this.dagScheduler = dagScheduler
    }

    /*
    * 重要的初始化函数，主要是在SparkContext中createTaskScheduler函数调用
    * */
    def initialize(backend: SchedulerBackend) {
        this.backend = backend
        // temporarily set rootPool name to empty
        /*
        *
        * Pool是调度池，rootPool是顶层的调度池，也就是根调度池，调度算法都是基于调度池的
        * https://blog.csdn.net/LINBE_blazers/article/details/92008509
        * 所谓的调度池pool是一个队列，def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
        * 组织关系：（此处用图片表示更好，链接中有图片）
        * 1、FIFOSchedulableBuilder：rootPool下面包含schedulableQueue，该队列包含着不同的TaskSetManager，每个TaskSetManager包含一个TaskSet
        * 2、
        * 在Pool.scala的40行定义的
        * */
        rootPool = new Pool("", schedulingMode, 0, 0)
        /*
        *
        * 调度池构建器，SchedulableBuilder是构建调度池Pool的构建器
        * */
        schedulableBuilder = {
            schedulingMode match {
                case SchedulingMode.FIFO =>
                    new FIFOSchedulableBuilder(rootPool)
                case SchedulingMode.FAIR =>
                    new FairSchedulableBuilder(rootPool, conf)
            }
        }
        schedulableBuilder.buildPools()
    }

    /*
    * 应该是在nextTaskId基础上，加1，形成新的ID号
    * */
    def newTaskId(): Long = nextTaskId.getAndIncrement()

    /*
    * TaskScheduler.start()->backend.start()
    * 看这部分代码，启动TaskScheduler也就是启动backend，TaskScheduler并没有启动什么东西
    * */
    override def start() {
        backend.start()

        if (!isLocal && conf.getBoolean("spark.speculation", false)) {
            /*
            * 开启推测执行
            * */
            logInfo("Starting speculative execution thread")
            import sc.env.actorSystem.dispatcher
            sc.env.actorSystem.scheduler.schedule(SPECULATION_INTERVAL milliseconds,
                SPECULATION_INTERVAL milliseconds) {
                Utils.tryOrExit {
                    checkSpeculatableTasks()
                }
            }
        }
    }

    override def postStartHook() {
        waitBackendReady()
    }

    /*
    *
    * 一个stage的tasks封装成TaskSet提交
    * TaskSet里面有val tasks: Array[Task[_]],
    * 根据TaskSet创建TaskSetManager，将TaskSetManager添加到schedulableBuilder
    * */
    override def submitTasks(taskSet: TaskSet) {
        val tasks = taskSet.tasks
        logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
        this.synchronized {
            /*
            *
            * 按照这个TaskSet创建一个TaskSetManager，一对一的
            * 还定义了task失败重试最大次数
            * */
            val manager = createTaskSetManager(taskSet, maxTaskFailures)
            activeTaskSets(taskSet.id) = manager
            /*
            *
            * 将TaskSetManager添加到schedulableBuilder中
            * */
            schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

            if (!isLocal && !hasReceivedTask) {
                starvationTimer.scheduleAtFixedRate(new TimerTask() {
                    override def run() {
                        if (!hasLaunchedTask) {
                            logWarning("Initial job has not accepted any resources; " +
                                    "check your cluster UI to ensure that workers are registered " +
                                    "and have sufficient resources")
                        } else {
                            this.cancel()
                        }
                    }
                }, STARVATION_TIMEOUT, STARVATION_TIMEOUT)
            }
            hasReceivedTask = true
        }
        /*
        *
        *executor.launchTask->tr = TaskRunner(taskid)->threadPool.execute(tr)
        *
        * */
        backend.reviveOffers()
    }

    // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
    private[scheduler] def createTaskSetManager(
                                                       taskSet: TaskSet,
                                                       maxTaskFailures: Int): TaskSetManager = {
        new TaskSetManager(this, taskSet, maxTaskFailures)
    }

    /*
    *
    * 删除task都是通过schedulerBackend做的，通过向backend的actor发送killtask命令，actor调用executor.killtask函数删除的；
    * */
    override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
        logInfo("Cancelling stage " + stageId)
        activeTaskSets.find(_._2.stageId == stageId).foreach { case (_, tsm) =>
            // There are two possible cases here:
            // 1. The task set manager has been created and some tasks have been scheduled.
            //    In this case, send a kill signal to the executors to kill the task and then abort
            //    the stage.
            // 2. The task set manager has been created but no tasks has been scheduled. In this case,
            //    simply abort the stage.
            tsm.runningTasksSet.foreach { tid =>
                val execId = taskIdToExecutorId(tid)
                /*
                *
                * 对backend的actor发送KillTask消息，然后调用executor.killtask函数，在线程中删除他
                * */
                backend.killTask(tid, execId, interruptThread)
            }
            tsm.abort("Stage %s cancelled".format(stageId))
            logInfo("Stage %d was cancelled".format(stageId))
        }
    }

    /**
      * Called to indicate that all task attempts (including speculated tasks) associated with the
      * given TaskSetManager have completed, so state associated with the TaskSetManager should be
      * cleaned up.
      */
    /*
    *
    * 表明该TaskSetManager里的所有task都完成了
    * */
    def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
        activeTaskSets -= manager.taskSet.id
        manager.parent.removeSchedulable(manager)
        logInfo("Removed TaskSet %s, whose tasks have all completed, from pool %s"
                .format(manager.taskSet.id, manager.parent.name))
    }

    /*
    *
    * */
    /*
    *
    * 该函数执行完后，更新了参数tasks: Seq[ArrayBuffer[TaskDescription]]，返回一个bool，应该表示启动了的task
    * */
    private def resourceOfferSingleTaskSet(
                                                  taskSet: TaskSetManager,
                                                  maxLocality: TaskLocality,
                                                  shuffledOffers: Seq[WorkerOffer],
                                                  availableCpus: Array[Int],
                                                  tasks: Seq[ArrayBuffer[TaskDescription]]): Boolean = {
        var launchedTask = false
        /*
        * 对每一个executor进行处理
        * */
        for (i <- 0 until shuffledOffers.size) {
            val execId = shuffledOffers(i).executorId
            val host = shuffledOffers(i).host
            if (availableCpus(i) >= CPUS_PER_TASK) {
                try {
                    /*
                    * taskSet.resourceOffer(execId, host, maxLocality)返回的是Option[TaskDescription]，也就是每个该TaskSet的每个task映射的executor号
                    * */
                    /*
                    *
                    * 这是TaskSet的resourceOffer，不是CoarseGrainedSchedulerBackend的resourceOffers
                    * */
                    for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
                        tasks(i) += task
                        val tid = task.taskId
                        taskIdToTaskSetId(tid) = taskSet.taskSet.id
                        taskIdToExecutorId(tid) = execId
                        executorsByHost(host) += execId
                        availableCpus(i) -= CPUS_PER_TASK
                        assert(availableCpus(i) >= 0)
                        launchedTask = true
                    }
                } catch {
                    case e: TaskNotSerializableException =>
                        logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
                        // Do not offer resources for this task, but don't throw an error to allow other
                        // task sets to be submitted.
                        return launchedTask
                }
            }
        }
        return launchedTask
    }

    /**
      * Called by cluster manager to offer resources on slaves. We respond by asking our active task
      * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
      * that tasks are balanced across the cluster.
      */
    /*
    *
    * CM调用去给slaves上用round-robin方式分配task，使得tasks均匀的分布在各个node节点上
    * WorkerOffer代表某个executor上空闲的资源
    * TaskDescription代表某个task被分配到哪个executor上执行
    *
    * resourceOffers最终返回每个task分配的executor信息的Seq集合
    *
    *
    * 对当前所有TaskSet处理
    * */
    def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
        // Mark each slave as alive and remember its hostname
        // Also track if new executor is added
        /*
        * 处理新的executor加入
        * */
        var newExecAvail = false
        for (o <- offers) {
            executorIdToHost(o.executorId) = o.host
            activeExecutorIds += o.executorId
            if (!executorsByHost.contains(o.host)) {
                executorsByHost(o.host) = new HashSet[String]()
                executorAdded(o.executorId, o.host)
                newExecAvail = true
            }
            for (rack <- getRackForHost(o.host)) {
                hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
            }
        }

        // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
        /*
        * 打乱顺序，使得Task均匀分配到各个worker节点上
        * */
        /*
        * 这一步是将executor乱序了
        * */
        val shuffledOffers = Random.shuffle(offers)
        // Build a list of tasks to assign to each worker.
        /*
        * 对每一个case class WorkerOffer(executorId: String, host: String, cores: Int)创建一个new ArrayBuffer[TaskDescription](o.cores)
        * 每个executor的ArrayBuffer有cores个元素，元素类型是TaskDescription
        * 因为每个core对应一个task
        * 意义：每个executor创建一个对tasks的映射
        * tasks = Seq(ArrayBuffer[TaskDescription](o.cores),ArrayBuffer[TaskDescription](o.cores),ArrayBuffer[TaskDescription](o.cores),ArrayBuffer[TaskDescription](o.cores).....)
        * availableCpus = Seq(o.cores, o.cores, o.cores, o.cores.......)
        * tasks和avaibaleCpus是一一对应的
        * */
        val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
        val availableCpus = shuffledOffers.map(o => o.cores).toArray
        /*
        *
        * 返回调度池中排序的TaskSetManager
        * 根据调度策略获取ArrayBuffer[TaskSetManager]
        * 返回override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {}
        * 其中rootPool在初始化就赋值了
        * */
        /*
        *
        * 该步骤直接拿到了当前提交到TaskScheduler里的所有taskSetManager，line 435行要对她们操作的！最后返回调用好的TaskDescription 所有task的对应关系
        * */
        val sortedTaskSets = rootPool.getSortedTaskSetQueue
        for (taskSet <- sortedTaskSets) {
            logDebug("parentName: %s, name: %s, runningTasks: %s".format(
                taskSet.parent.name, taskSet.name, taskSet.runningTasks))
            if (newExecAvail) {
                /*
                * 如果有新加入的executor，就添加到TaskSet中
                * */
                taskSet.executorAdded()
            }
        }

        // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
        // of locality levels so that it gets a chance to launch local tasks on all of them.
        // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
        /*
        * 按就近原则进行Task调度
        * */
        var launchedTask = false
        /*
        * 其中taskSet是TaskSetManager
        * */
        for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
            do {
                /*
                * 该函数执行完后，更新了参数tasks: Seq[ArrayBuffer[TaskDescription]]，返回一个bool，应该表示启动了的task
                * 最后一个参数tasks被更新了，在line 396行定义的
                * */
                launchedTask = resourceOfferSingleTaskSet(
                    taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
            } while (launchedTask)
        }

        if (tasks.size > 0) {
            hasLaunchedTask = true
        }
        return tasks
    }

    def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
        var failedExecutor: Option[String] = None
        synchronized {
            try {
                if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
                    // We lost this entire executor, so remember that it's gone
                    val execId = taskIdToExecutorId(tid)
                    if (activeExecutorIds.contains(execId)) {
                        removeExecutor(execId)
                        failedExecutor = Some(execId)
                    }
                }
                taskIdToTaskSetId.get(tid) match {
                    case Some(taskSetId) =>
                        if (TaskState.isFinished(state)) {
                            taskIdToTaskSetId.remove(tid)
                            taskIdToExecutorId.remove(tid)
                        }
                        activeTaskSets.get(taskSetId).foreach { taskSet =>
                            if (state == TaskState.FINISHED) {
                                taskSet.removeRunningTask(tid)
                                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
                            } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                                taskSet.removeRunningTask(tid)
                                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
                            }
                        }
                    case None =>
                        logError(
                            ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                                    "likely the result of receiving duplicate task finished status updates)")
                                    .format(state, tid))
                }
            } catch {
                case e: Exception => logError("Exception in statusUpdate", e)
            }
        }
        // Update the DAGScheduler without holding a lock on this, since that can deadlock
        if (failedExecutor.isDefined) {
            dagScheduler.executorLost(failedExecutor.get)
            backend.reviveOffers()
        }
    }

    /**
      * Update metrics for in-progress tasks and let the master know that the BlockManager is still
      * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
      * indicating that the block manager should re-register.
      */
    override def executorHeartbeatReceived(
                                                  execId: String,
                                                  taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics
                                                  blockManagerId: BlockManagerId): Boolean = {

        val metricsWithStageIds: Array[(Long, Int, Int, TaskMetrics)] = synchronized {
            taskMetrics.flatMap { case (id, metrics) =>
                taskIdToTaskSetId.get(id)
                        .flatMap(activeTaskSets.get)
                        .map(taskSetMgr => (id, taskSetMgr.stageId, taskSetMgr.taskSet.attempt, metrics))
            }
        }
        dagScheduler.executorHeartbeatReceived(execId, metricsWithStageIds, blockManagerId)
    }

    def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
        taskSetManager.handleTaskGettingResult(tid)
    }

    def handleSuccessfulTask(
                                    taskSetManager: TaskSetManager,
                                    tid: Long,
                                    taskResult: DirectTaskResult[_]) = synchronized {
        taskSetManager.handleSuccessfulTask(tid, taskResult)
    }

    def handleFailedTask(
                                taskSetManager: TaskSetManager,
                                tid: Long,
                                taskState: TaskState,
                                reason: TaskEndReason) = synchronized {
        taskSetManager.handleFailedTask(tid, taskState, reason)
        if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
            // Need to revive offers again now that the task set manager state has been updated to
            // reflect failed tasks that need to be re-run.
            backend.reviveOffers()
        }
    }

    def error(message: String) {
        synchronized {
            if (activeTaskSets.size > 0) {
                // Have each task set throw a SparkException with the error
                for ((taskSetId, manager) <- activeTaskSets) {
                    try {
                        manager.abort(message)
                    } catch {
                        case e: Exception => logError("Exception in error callback", e)
                    }
                }
            } else {
                // No task sets are active but we still got an error. Just exit since this
                // must mean the error is during registration.
                // It might be good to do something smarter here in the future.
                logError("Exiting due to error from cluster scheduler: " + message)
                System.exit(1)
            }
        }
    }

    /*
    *
    * 关闭TaskScheduler就是关闭schedulerBackend
    * */
    override def stop() {
        if (backend != null) {
            backend.stop()
        }
        if (taskResultGetter != null) {
            taskResultGetter.stop()
        }
        starvationTimer.cancel()
    }

    /*
    * 默认并行度是调用TaskScheduler.defaultParallelism()，内部调用backend.defaultParallelism()
    * */
    override def defaultParallelism() = backend.defaultParallelism()

    // Check for speculatable tasks in all our active jobs.
    def checkSpeculatableTasks() {
        var shouldRevive = false
        synchronized {
            shouldRevive = rootPool.checkSpeculatableTasks()
        }
        if (shouldRevive) {
            backend.reviveOffers()
        }
    }

    def executorLost(executorId: String, reason: ExecutorLossReason) {
        var failedExecutor: Option[String] = None

        synchronized {
            if (activeExecutorIds.contains(executorId)) {
                val hostPort = executorIdToHost(executorId)
                logError("Lost executor %s on %s: %s".format(executorId, hostPort, reason))
                removeExecutor(executorId)
                failedExecutor = Some(executorId)
            } else {
                // We may get multiple executorLost() calls with different loss reasons. For example, one
                // may be triggered by a dropped connection from the slave while another may be a report
                // of executor termination from Mesos. We produce log messages for both so we eventually
                // report the termination reason.
                logError("Lost an executor " + executorId + " (already removed): " + reason)
            }
        }
        // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
        if (failedExecutor.isDefined) {
            dagScheduler.executorLost(failedExecutor.get)
            backend.reviveOffers()
        }
    }

    /** Remove an executor from all our data structures and mark it as lost */
    private def removeExecutor(executorId: String) {
        activeExecutorIds -= executorId
        val host = executorIdToHost(executorId)
        val execs = executorsByHost.getOrElse(host, new HashSet)
        execs -= executorId
        if (execs.isEmpty) {
            executorsByHost -= host
            for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
                hosts -= host
                if (hosts.isEmpty) {
                    hostsByRack -= rack
                }
            }
        }
        executorIdToHost -= executorId
        rootPool.executorLost(executorId, host)
    }

    def executorAdded(execId: String, host: String) {
        dagScheduler.executorAdded(execId, host)
    }

    def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
        executorsByHost.get(host).map(_.toSet)
    }

    def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
        executorsByHost.contains(host)
    }

    def hasHostAliveOnRack(rack: String): Boolean = synchronized {
        hostsByRack.contains(rack)
    }

    def isExecutorAlive(execId: String): Boolean = synchronized {
        activeExecutorIds.contains(execId)
    }

    // By default, rack is unknown
    def getRackForHost(value: String): Option[String] = None

    private def waitBackendReady(): Unit = {
        if (backend.isReady) {
            return
        }
        while (!backend.isReady) {
            synchronized {
                this.wait(100)
            }
        }
    }

    override def applicationId(): String = backend.applicationId()

}


private[spark] object TaskSchedulerImpl {
    /**
      * Used to balance containers across hosts.
      *
      * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
      * resource offers representing the order in which the offers should be used.  The resource
      * offers are ordered such that we'll allocate one container on each host before allocating a
      * second container on any host, and so on, in order to reduce the damage if a host fails.
      *
      * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
      * [o1, o5, o4, 02, o6, o3]
      */
    def prioritizeContainers[K, T](map: HashMap[K, ArrayBuffer[T]]): List[T] = {
        val _keyList = new ArrayBuffer[K](map.size)
        _keyList ++= map.keys

        // order keyList based on population of value in map
        val keyList = _keyList.sortWith(
            (left, right) => map(left).size > map(right).size
        )

        val retval = new ArrayBuffer[T](keyList.size * 2)
        var index = 0
        var found = true

        while (found) {
            found = false
            for (key <- keyList) {
                val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null)
                assert(containerList != null)
                // Get the index'th entry for this host - if present
                if (index < containerList.size) {
                    retval += containerList.apply(index)
                    found = true
                }
            }
            index += 1
        }

        retval.toList
    }

}
