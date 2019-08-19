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

package org.apache.spark.deploy.master

import java.io.FileNotFoundException
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import akka.pattern.ask
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{
    ApplicationDescription, DriverDescription,
    ExecutorState, SparkHadoopUtil
}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, SignalLogger, Utils}

//master和worker一样，是个actor，并且继承了多master选举的trait
private[spark] class Master(
                                   host: String,
                                   port: Int,
                                   webUiPort: Int,
                                   val securityMgr: SecurityManager,
                                   val conf: SparkConf)
        extends Actor with ActorLogReceive with Logging with LeaderElectable {

    //这里直接调用actor trait中的变量
    import context.dispatcher // to use Akka's scheduler.schedule()

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

    def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss") // For application IDs
    val WORKER_TIMEOUT = conf.getLong("spark.worker.timeout", 60) * 1000
    val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
    val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
    val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)

    /*
    * master的高可用恢复模式：1、zookeeper；2、filesystem；3、custom
    * 可以将master相关数据保存用于某个master失效后其余master的恢复
    * */
    val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")


    //两个数据结构：WorkerInfo和ApplicationInfo
    //主要三类：worker、app、driver
    val workers = new HashSet[WorkerInfo]
    val idToWorker = new HashMap[String, WorkerInfo]
    val addressToWorker = new HashMap[Address, WorkerInfo]

    val apps = new HashSet[ApplicationInfo]
    val idToApp = new HashMap[String, ApplicationInfo]
    val actorToApp = new HashMap[ActorRef, ApplicationInfo]
    val addressToApp = new HashMap[Address, ApplicationInfo]
    val waitingApps = new ArrayBuffer[ApplicationInfo]
    val completedApps = new ArrayBuffer[ApplicationInfo]
    var nextAppNumber = 0
    val appIdToUI = new HashMap[String, SparkUI]

    val drivers = new HashSet[DriverInfo]
    val completedDrivers = new ArrayBuffer[DriverInfo]
    val waitingDrivers = new ArrayBuffer[DriverInfo] // Drivers currently spooled for scheduling
    var nextDriverNumber = 0

    Utils.checkHost(host, "Expected hostname")

    //创建统计系统，统计相关的略过，监控的东西不一样，一个是监控master，一个是监控application
    val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
    val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
        securityMgr)
    //监控三种：worker数量、application数量、等待application数量
    val masterSource = new MasterSource(this)

    val webUi = new MasterWebUI(this, webUiPort)

    //从sparkConf中获取master主IP
    val masterPublicAddress = {
        val envVar = conf.getenv("SPARK_PUBLIC_DNS")
        if (envVar != null) envVar else host
    }

    val masterUrl = "spark://" + host + ":" + port
    var masterWebUiUrl: String = _

    var state = RecoveryState.STANDBY

    //不知道持久化的引擎是什么东西？？？？
    //度量系统，用来恢复master的
    //一个操作持久化的变量
    var persistenceEngine: PersistenceEngine = _

    //多master的leader竞选
    //一个操作竞选机制的变量
    var leaderElectionAgent: LeaderElectionAgent = _

    private var recoveryCompletionTask: Cancellable = _

    // As a temporary workaround before better ways of configuring memory, we allow users to set
    // a flag that will perform round-robin scheduling across the nodes (spreading out each app
    // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.


    /*
    *默认application资源分配是均匀打散到各个node上，round-robin模式，这些都可以在sparkConf中配置，或者submit提交时修改
    * round-robin是每个node顺序分配一个core，多次轮训
    * consolidate是依次打满每个node的core
    * */
    val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

    // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
    val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
    if (defaultCores < 1) {
        throw new SparkException("spark.deploy.defaultCores must be positive")
    }

    // Alternative application submission gateway that is stable across Spark versions
    private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
    private val restServer =
        if (restServerEnabled) {
            val port = conf.getInt("spark.master.rest.port", 6066)
            Some(new StandaloneRestServer(host, port, self, masterUrl, conf))
        } else {
            None
        }
    private val restServerBoundPort = restServer.map(_.start())

    //依然是actor的三个重写方法
    /*
    * 1、注册相关系统，如metrics、web等；
    * 2、检查worker是否超时；
    * 3、创建两个变量：（1）persistenceEngine变量用来持久化恢复（2）leaderElectionAgent变量用来竞选新的master
    * */
    override def preStart() {
        logInfo("Starting Spark master at " + masterUrl)
        logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
        // Listen for remote client disconnection events, since they don't go through Akka's watch()
        context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
        webUi.bind()
        masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
        //每隔WORKER_TIMEOUT时间向master发送CheckForWorkerTimeOut消息检查worker是否超时
        context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis, self, CheckForWorkerTimeOut)

        /*
        * 向监控系统注册master资源并启动监控，开始监控master和application相关信息
        * */
        masterMetricsSystem.registerSource(masterSource)
        masterMetricsSystem.start()
        applicationMetricsSystem.start()
        // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
        // started.
        masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
        applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)


        //仅仅是根据持久化的模式，构建了（1）持久化和（2）竞选机制的一个对象，并没有真正的开始持久化或恢复操作
        //1、zookeeper；2、filesystem；3、custom
        //持久化都需要序列化
        val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
            case "ZOOKEEPER" =>
                logInfo("Persisting recovery state to ZooKeeper")
                val zkFactory =
                    new ZooKeeperRecoveryModeFactory(conf, SerializationExtension(context.system))
                //仅仅是构建了持久化的一个对象，并没有真正的开始持久化或恢复操作
                (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
            case "FILESYSTEM" =>
                val fsFactory =
                    new FileSystemRecoveryModeFactory(conf, SerializationExtension(context.system))
                (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
            case "CUSTOM" =>
                val clazz = Class.forName(conf.get("spark.deploy.recoveryMode.factory"))
                val factory = clazz.getConstructor(conf.getClass, Serialization.getClass)
                        .newInstance(conf, SerializationExtension(context.system))
                        .asInstanceOf[StandaloneRecoveryModeFactory]
                (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
            case _ =>
                (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
        }
        persistenceEngine = persistenceEngine_
        leaderElectionAgent = leaderElectionAgent_
    }

    override def preRestart(reason: Throwable, message: Option[Any]) {
        super.preRestart(reason, message) // calls postStop()!
        logError("Master actor restarted due to exception", reason)
    }

    override def postStop() {
        masterMetricsSystem.report()
        applicationMetricsSystem.report()
        // prevent the CompleteRecovery message sending to restarted master
        if (recoveryCompletionTask != null) {
            recoveryCompletionTask.cancel()
        }
        webUi.stop()
        restServer.foreach(_.stop())
        masterMetricsSystem.stop()
        applicationMetricsSystem.stop()
        persistenceEngine.close()
        leaderElectionAgent.stop()
    }

    //向master这个actor发送竞选命令，这个message在receiveWithLogging中有处理过程，250行，该方法只是继承了leaderagent相关的trait
    override def electedLeader() {
        self ! ElectedLeader
    }

    //向master这个actor发送移除leader的命令，移除master的leader
    override def revokedLeadership() {
        self ! RevokedLeadership
    }


    /*
    * 核心函数：消息处理机制
    * */
    override def receiveWithLogging = {
        case ElectedLeader => {
            /*
            * 所谓的竞选leader，就是从持久化的数据中恢复数据？？？？竞选过程在哪里？
            * 貌似，这仅仅就是主备切换
            * 这个属于，master被选举为leader后发送的electedLeader消息，表示已经选举完成，所以这里需要从持久化数据中恢复数据
            * */
            val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData()
            state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
                RecoveryState.ALIVE
            } else {
                RecoveryState.RECOVERING
            }
            logInfo("I have been elected leader! New state: " + state)
            //竞选完后要像master发送恢复完成的消息
            if (state == RecoveryState.RECOVERING) {
                beginRecovery(storedApps, storedDrivers, storedWorkers)
                recoveryCompletionTask = context.system.scheduler.scheduleOnce(WORKER_TIMEOUT millis, self,
                    CompleteRecovery)
            }
        }

        case CompleteRecovery => completeRecovery()

        case RevokedLeadership => {
            logError("Leadership has been revoked -- master shutting down.")
            System.exit(0)
        }

        case RegisterWorker(id, workerHost, workerPort, cores, memory, workerUiPort, publicAddress) => {
            logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
                workerHost, workerPort, cores, Utils.megabytesToString(memory)))
            /*
            * 如果该master处于standby模式，则不回应
            * */
            if (state == RecoveryState.STANDBY) {
                // ignore, don't send response
            } else if (idToWorker.contains(id)) {
                /*
                * 如果该worker已经在master的内存里，则不允许重复注册
                * */
                sender ! RegisterWorkerFailed("Duplicate worker ID")
            } else {
                /*
                * 否则，才正常注册
                * 所谓注册就是，将worker信息封装成workerInfo
                * */
                val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
                    sender, workerUiPort, publicAddress)
                if (registerWorker(worker)) {
                    //addWorker调用了persist()方法，将worker信息持久化写入XXXX
                    persistenceEngine.addWorker(worker)
                    /*
                    * 发送已注册消息
                    * 由于59行该Worker继承了Actor，所以可以直接调用Actor的sender方法
                    * 研究一下sender到底是worker还是master的actor！！！！！
                    * */
                    sender ! RegisteredWorker(masterUrl, masterWebUiUrl)
                    /*
                    * 每次资源改变或者新APP加入时候，就会调用这个schedule()方法
                    * */
                    schedule()
                } else {
                    val workerAddress = worker.actor.path.address
                    logWarning("Worker registration failed. Attempted to re-register worker at same " +
                            "address: " + workerAddress)
                    sender ! RegisterWorkerFailed("Attempted to re-register worker at same address: "
                            + workerAddress)
                }
            }
        }

        //description就是DriverDescription，创建driver
        //该消息是由Client.class文件中的CLientActor类提交的，通过引用master actor提交给master的actor的
        //DriverClient->master
        case RequestSubmitDriver(description) => {
            //master的固定逻辑，首先检查自己这个maser有没有活着
            if (state != RecoveryState.ALIVE) {
                val msg = s"Can only accept driver submissions in ALIVE state. Current state: $state."
                sender ! SubmitDriverResponse(false, None, msg)
            } else {
                logInfo("Driver submitted " + description.command.mainClass)
                /*
                * 这里开始创建Driver
                * */
                val driver = createDriver(description)
                persistenceEngine.addDriver(driver)
                waitingDrivers += driver
                /*
                * drivers数据结构里，增加了一个新的driver
                * */
                drivers.add(driver)
                schedule()

                // TODO: It might be good to instead have the submission client poll the master to determine
                //       the current status of the driver. For now it's simply "fire and forget".

                sender ! SubmitDriverResponse(true, Some(driver.id),
                    s"Driver successfully submitted as ${driver.id}")
            }
        }

        case RequestKillDriver(driverId) => {
            if (state != RecoveryState.ALIVE) {
                val msg = s"Can only kill drivers in ALIVE state. Current state: $state."
                sender ! KillDriverResponse(driverId, success = false, msg)
            } else {
                logInfo("Asked to kill driver " + driverId)
                /*
                * 在自己的drivers数据结构中查找需要删除的driver
                * val drivers = new HashSet[DriverInfo]
                * */
                val driver = drivers.find(_.id == driverId)
                driver match {
                    //如果存在
                    case Some(d) =>
                        //如果在等待运行的序列，那么直接在val waitingDrivers = new ArrayBuffer[DriverInfo] // Drivers currently spooled for scheduling中删除即可，
                        // 再通知一下driver状态改变
                        if (waitingDrivers.contains(d)) {
                            waitingDrivers -= d
                            self ! DriverStateChanged(driverId, DriverState.KILLED, None)
                        } else {
                            //如果该driver正在运行中
                            // We just notify the worker to kill the driver here. The final bookkeeping occurs
                            // on the return path when the worker submits a state change back to the master
                            // to notify it that the driver was successfully killed.
                            d.worker.foreach { w =>
                                //向每个worker的actor发KillDriver消息，
                                //让Worker去杀死Driver
                                w.actor ! KillDriver(driverId)
                            }
                        }
                        // TODO: It would be nice for this to be a synchronous response
                        val msg = s"Kill request for $driverId submitted"
                        logInfo(msg)
                        sender ! KillDriverResponse(driverId, success = true, msg)
                    //如果不存在，说明该准备删除的driver意思运行结束退出或者不存在
                    case None =>
                        val msg = s"Driver $driverId has already finished or does not exist"
                        logWarning(msg)
                        sender ! KillDriverResponse(driverId, success = false, msg)
                }
            }
        }

        case RequestDriverStatus(driverId) => {
            (drivers ++ completedDrivers).find(_.id == driverId) match {
                case Some(driver) =>
                    //直接调用driver数据结构里的state，封装在消息中发送出去
                    sender ! DriverStatusResponse(found = true, Some(driver.state),
                        driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception)
                case None =>
                    sender ! DriverStatusResponse(found = false, None, None, None, None)
            }
        }

        // AppClient to Master
        case RegisterApplication(description) => {
            if (state == RecoveryState.STANDBY) {
                // ignore, don't send response
            } else {
                logInfo("Registering app " + description.name)
                val app = createApplication(description, sender)
                registerApplication(app)
                logInfo("Registered app " + description.name + " with ID " + app.id)
                persistenceEngine.addApplication(app)
                sender ! RegisteredApplication(app.id, masterUrl)
                /*
                *
                *
                *
                *
                * 该调度函数schedule()非常非常重要！！！
                *
                *
                *
                *
                * */
                schedule()
            }
        }

        case ExecutorStateChanged(appId, execId, state, message, exitStatus) => {
            val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
            execOption match {
                case Some(exec) => {
                    val appInfo = idToApp(appId)
                    exec.state = state
                    if (state == ExecutorState.RUNNING) {
                        appInfo.resetRetryCount()
                    }
                    exec.application.driver ! ExecutorUpdated(execId, state, message, exitStatus)
                    if (ExecutorState.isFinished(state)) {
                        // Remove this executor from the worker and app
                        logInfo(s"Removing executor ${exec.fullId} because it is $state")
                        appInfo.removeExecutor(exec)
                        exec.worker.removeExecutor(exec)

                        val normalExit = exitStatus == Some(0)
                        // Only retry certain number of times so we don't go into an infinite loop.
                        if (!normalExit) {
                            if (appInfo.incrementRetryCount() < ApplicationState.MAX_NUM_RETRY) {
                                schedule()
                            } else {
                                val execs = appInfo.executors.values
                                if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                                    logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                                            s"${appInfo.retryCount} times; removing it")
                                    removeApplication(appInfo, ApplicationState.FAILED)
                                }
                            }
                        }
                    }
                }
                case None =>
                    logWarning(s"Got status update for unknown executor $appId/$execId")
            }
        }

        /*
        * 总结一下：一旦收到stateChange相关消息，只有两种方案：
        * 1、如果在队列里，还没运行，则直接把存储的数据结构中该Driver等删除，通知；2、如果已经运行了，kill线程，清除目录，通知
        * */
        case DriverStateChanged(driverId, state, exception) => {
            state match {
                case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
                    removeDriver(driverId, state, exception)
                case _ =>
                    throw new Exception(s"Received unexpected state update for driver $driverId: $state")
            }
        }

        case Heartbeat(workerId) => {
            idToWorker.get(workerId) match {
                case Some(workerInfo) =>
                    //记录该worker最新心跳
                    workerInfo.lastHeartbeat = System.currentTimeMillis()
                case None =>
                    if (workers.map(_.id).contains(workerId)) {
                        logWarning(s"Got heartbeat from unregistered worker $workerId." +
                                " Asking it to re-register.")
                        sender ! ReconnectWorker(masterUrl)
                    } else {
                        logWarning(s"Got heartbeat from unregistered worker $workerId." +
                                " This worker was never registered, so ignoring the heartbeat.")
                    }
            }
        }

        case MasterChangeAcknowledged(appId) => {
            idToApp.get(appId) match {
                case Some(app) =>
                    logInfo("Application has been re-registered: " + appId)
                    app.state = ApplicationState.WAITING
                case None =>
                    logWarning("Master change ack from unknown app: " + appId)
            }

            if (canCompleteRecovery) {
                completeRecovery()
            }
        }

        case WorkerSchedulerStateResponse(workerId, executors, driverIds) => {
            idToWorker.get(workerId) match {
                case Some(worker) =>
                    logInfo("Worker has been re-registered: " + workerId)
                    worker.state = WorkerState.ALIVE

                    val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
                    for (exec <- validExecutors) {
                        val app = idToApp.get(exec.appId).get
                        val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
                        worker.addExecutor(execInfo)
                        execInfo.copyState(exec)
                    }

                    for (driverId <- driverIds) {
                        drivers.find(_.id == driverId).foreach { driver =>
                            driver.worker = Some(worker)
                            driver.state = DriverState.RUNNING
                            worker.drivers(driverId) = driver
                        }
                    }
                case None =>
                    logWarning("Scheduler state from unknown worker: " + workerId)
            }

            if (canCompleteRecovery) {
                completeRecovery()
            }
        }

        case DisassociatedEvent(_, address, _) => {
            // The disconnected client could've been either a worker or an app; remove whichever it was
            logInfo(s"$address got disassociated, removing it.")
            addressToWorker.get(address).foreach(removeWorker)
            addressToApp.get(address).foreach(finishApplication)
            if (state == RecoveryState.RECOVERING && canCompleteRecovery) {
                completeRecovery()
            }
        }

        case RequestMasterState => {
            sender ! MasterStateResponse(
                host, port, restServerBoundPort,
                workers.toArray, apps.toArray, completedApps.toArray,
                drivers.toArray, completedDrivers.toArray, state)
        }

        case CheckForWorkerTimeOut => {
            timeOutDeadWorkers()
        }

        case BoundPortsRequest => {
            sender ! BoundPortsResponse(port, webUi.boundPort, restServerBoundPort)
        }
    }

    def canCompleteRecovery =
        workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
                apps.count(_.state == ApplicationState.UNKNOWN) == 0

    //分别恢复application、driver、worker
    //恢复，也是重新注册，维护这三者的数据结构而已
    def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
                      storedWorkers: Seq[WorkerInfo]) {
        for (app <- storedApps) {
            logInfo("Trying to recover app: " + app.id)
            try {
                //所谓恢复，就是重新注册，保存变量到apps变量里
                registerApplication(app)
                app.state = ApplicationState.UNKNOWN
                app.driver ! MasterChanged(masterUrl, masterWebUiUrl)
            } catch {
                case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
            }
        }

        for (driver <- storedDrivers) {
            // Here we just read in the list of drivers. Any drivers associated with now-lost workers
            // will be re-launched when we detect that the worker is missing.
            drivers += driver
        }

        for (worker <- storedWorkers) {
            logInfo("Trying to recover worker: " + worker.id)
            try {
                registerWorker(worker)
                worker.state = WorkerState.UNKNOWN
                worker.actor ! MasterChanged(masterUrl, masterWebUiUrl)
            } catch {
                case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
            }
        }
    }

    //加锁，更新数据结构，移除恢复过程中发现的有问题的组件
    def completeRecovery() {
        // Ensure "only-once" recovery semantics using a short synchronization period.
        synchronized {
            if (state != RecoveryState.RECOVERING) {
                return
            }
            state = RecoveryState.COMPLETING_RECOVERY
        }

        /*
        * 移除了那些有问题，超时的worker 、driver 、apps
        * */
        // Kill off any workers and apps that didn't respond to us.
        workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
        apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

        // Reschedule drivers which were not claimed by any workers
        drivers.filter(_.worker.isEmpty).foreach { d =>
            logWarning(s"Driver ${d.id} was not found after master recovery")
            if (d.desc.supervise) {
                logWarning(s"Re-launching ${d.id}")
                relaunchDriver(d)
            } else {
                removeDriver(d.id, DriverState.ERROR, None)
                logWarning(s"Did not re-launch ${d.id} because it was not supervised")
            }
        }

        state = RecoveryState.ALIVE
        schedule()
        logInfo("Recovery complete - resuming operations!")
    }

    /**
      * Can an app use the given worker? True if the worker has enough memory and we haven't already
      * launched an executor for the app on it (right now the standalone backend doesn't like having
      * two executors on the same worker).
      */
    //如果该worker上的资源大于该applicationDescription资源，则true，可以使用
    def canUse(app: ApplicationInfo, worker: WorkerInfo): Boolean = {
        worker.memoryFree >= app.desc.memoryPerSlave && !worker.hasExecutor(app)
    }

    /**
      * Schedule the currently available resources among waiting apps. This method will be called
      * every time a new app joins or resource availability changes.
      */
    //根据分配方式，如spreadOutApps，给新app调度资源，
    // 给app分配worker和executor,启动对应的Executor，launchExecutor(usableWorkers(pos), exec)
    /** ******************这个函数非常重要 *********************************/
    private def schedule() {
        if (state != RecoveryState.ALIVE) {
            return
        }

        // First schedule drivers, they take strict precedence over applications
        // Randomization helps balance drivers
        val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
        val numWorkersAlive = shuffledAliveWorkers.size
        var curPos = 0

        /*
        *
        * 首先启动一个个等待调度的app对应的Driver
        * */
        for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
            // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
            // start from the last worker that was assigned a driver, and continue onwards until we have
            // explored all alive workers.
            var launched = false
            var numWorkersVisited = 0
            while (numWorkersVisited < numWorkersAlive && !launched) {
                val worker = shuffledAliveWorkers(curPos)
                numWorkersVisited += 1
                if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
                    /*
                    * 启动driver线程，
                    * 向work的actor发送LaunchDriver信号
                    * */
                    launchDriver(worker, driver)
                    waitingDrivers -= driver
                    launched = true
                }
                curPos = (curPos + 1) % numWorkersAlive
            }
        }

        // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
        // in the queue, then the second app, etc.
        /*
        *
        * 下面开始给app分executor、cores费喷资源，调度app过程
        * 对app进行FIFO原则，也就是分摊到尽可能多的节点上
        * */
        if (spreadOutApps) {
            // Try to spread out each app among all the nodes, until it has all its cores
            for (app <- waitingApps if app.coresLeft > 0) {
                //从保存的数据结构workers中，找到资源从多少的worker排序
                //其实所有的操作，都是在维护这些数据结构！！！各种信息，也都可以从数据结构里获得
                val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
                        .filter(canUse(app, _)).sortBy(_.coresFree).reverse
                val numUsable = usableWorkers.length
                val assigned = new Array[Int](numUsable) // Number of cores to give on each node
                var toAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
                var pos = 0
                while (toAssign > 0) {
                    if (usableWorkers(pos).coresFree - assigned(pos) > 0) {
                        toAssign -= 1
                        assigned(pos) += 1
                    }
                    pos = (pos + 1) % numUsable
                }
                // Now that we've decided how many cores to give on each node, let's actually give them
                for (pos <- 0 until numUsable) {
                    if (assigned(pos) > 0) {
                        val exec = app.addExecutor(usableWorkers(pos), assigned(pos))
                        launchExecutor(usableWorkers(pos), exec)
                        app.state = ApplicationState.RUNNING
                    }
                }
            }
        } else {
            /*
            * 这种方式和上面的方式的区别是，这种方式尽量可能用到少量的节点完成这个任务
            * */
            // Pack each app into as few nodes as possible until we've assigned all its cores
            for (worker <- workers if worker.coresFree > 0 && worker.state == WorkerState.ALIVE) {
                for (app <- waitingApps if app.coresLeft > 0) {
                    if (canUse(app, worker)) {
                        val coresToUse = math.min(worker.coresFree, app.coresLeft)
                        if (coresToUse > 0) {
                            val exec = app.addExecutor(worker, coresToUse)
                            //分配worker和executor
                            launchExecutor(worker, exec)
                            app.state = ApplicationState.RUNNING
                        }
                    }
                }
            }
        }
    }

    /*
    * 启动某个worker上的某个executor
    * */
    def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc) {
        logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
        /*
        * 给该worker的executors数据结构添加executor
        * */
        worker.addExecutor(exec)
        /*
        * 给worker发送启动Executor的命令
        * */
        worker.actor ! LaunchExecutor(masterUrl,
            exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory)
        exec.application.driver ! ExecutorAdded(
            exec.id, worker.id, worker.hostPort, exec.cores, exec.memory)
    }

    def registerWorker(worker: WorkerInfo): Boolean = {
        // There may be one or more refs to dead workers on this same node (w/ different ID's),
        // remove them.
        workers.filter { w =>
            (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
        }.foreach { w =>
            workers -= w
        }

        val workerAddress = worker.actor.path.address
        if (addressToWorker.contains(workerAddress)) {
            val oldWorker = addressToWorker(workerAddress)
            if (oldWorker.state == WorkerState.UNKNOWN) {
                // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
                // The old worker must thus be dead, so we will remove it and accept the new worker.
                removeWorker(oldWorker)
            } else {
                logInfo("Attempted to re-register worker at same address: " + workerAddress)
                return false
            }
        }

        workers += worker
        idToWorker(worker.id) = worker
        addressToWorker(workerAddress) = worker
        true
    }

    def removeWorker(worker: WorkerInfo) {
        logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
        /*
        * 标记当前work的状态为“dead”
        * */
        worker.setState(WorkerState.DEAD)
        /*
        * 都是把信息从自己维护的数据结构中删除
        * */
        idToWorker -= worker.id
        addressToWorker -= worker.actor.path.address
        for (exec <- worker.executors.values) {
            logInfo("Telling app of lost executor: " + exec.id)
            exec.application.driver ! ExecutorUpdated(
                exec.id, ExecutorState.LOST, Some("worker lost"), None)
            exec.application.removeExecutor(exec)
        }
        for (driver <- worker.drivers.values) {
            if (driver.desc.supervise) {
                logInfo(s"Re-launching ${driver.id}")
                relaunchDriver(driver)
            } else {
                logInfo(s"Not re-launching ${driver.id} because it was not supervised")
                removeDriver(driver.id, DriverState.ERROR, None)
            }
        }
        persistenceEngine.removeWorker(worker)
    }

    def relaunchDriver(driver: DriverInfo) {
        driver.worker = None
        driver.state = DriverState.RELAUNCHING
        waitingDrivers += driver
        schedule()
    }

    def createApplication(desc: ApplicationDescription, driver: ActorRef): ApplicationInfo = {
        val now = System.currentTimeMillis()
        val date = new Date(now)
        new ApplicationInfo(now, newApplicationId(date), desc, date, driver, defaultCores)
    }

    //所谓的注册application，就是把APP的信息保存在自己的apps内存变量里而已
    //val apps = new HashSet[ApplicationInfo]
    def registerApplication(app: ApplicationInfo): Unit = {
        val appAddress = app.driver.path.address
        if (addressToApp.contains(appAddress)) {
            logInfo("Attempted to re-register application at same address: " + appAddress)
            return
        }

        applicationMetricsSystem.registerSource(app.appSource)
        apps += app
        idToApp(app.id) = app
        actorToApp(app.driver) = app
        addressToApp(appAddress) = app
        waitingApps += app
    }

    def finishApplication(app: ApplicationInfo) {
        removeApplication(app, ApplicationState.FINISHED)
    }

    def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
        if (apps.contains(app)) {
            logInfo("Removing app " + app.id)
            apps -= app
            idToApp -= app.id
            actorToApp -= app.driver
            addressToApp -= app.driver.path.address
            if (completedApps.size >= RETAINED_APPLICATIONS) {
                val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
                completedApps.take(toRemove).foreach(a => {
                    appIdToUI.remove(a.id).foreach { ui => webUi.detachSparkUI(ui) }
                    applicationMetricsSystem.removeSource(a.appSource)
                })
                completedApps.trimStart(toRemove)
            }
            completedApps += app // Remember it in our history
            waitingApps -= app

            // If application events are logged, use them to rebuild the UI
            rebuildSparkUI(app)

            for (exec <- app.executors.values) {
                exec.worker.removeExecutor(exec)
                exec.worker.actor ! KillExecutor(masterUrl, exec.application.id, exec.id)
                exec.state = ExecutorState.KILLED
            }
            app.markFinished(state)
            if (state != ApplicationState.FINISHED) {
                app.driver ! ApplicationRemoved(state.toString)
            }
            persistenceEngine.removeApplication(app)
            schedule()

            // Tell all workers that the application has finished, so they can clean up any app state.
            workers.foreach { w =>
                w.actor ! ApplicationFinished(app.id)
            }
        }
    }

    /**
      * Rebuild a new SparkUI from the given application's event logs.
      * Return whether this is successful.
      */
    def rebuildSparkUI(app: ApplicationInfo): Boolean = {
        val appName = app.desc.name
        val notFoundBasePath = HistoryServer.UI_PATH_PREFIX + "/not-found"
        try {
            val eventLogFile = app.desc.eventLogDir
                    .map { dir => EventLoggingListener.getLogPath(dir, app.id, app.desc.eventLogCodec) }
                    .getOrElse {
                        // Event logging is not enabled for this application
                        app.desc.appUiUrl = notFoundBasePath
                        return false
                    }

            val fs = Utils.getHadoopFileSystem(eventLogFile, hadoopConf)

            if (fs.exists(new Path(eventLogFile + EventLoggingListener.IN_PROGRESS))) {
                // Event logging is enabled for this application, but the application is still in progress
                val title = s"Application history not found (${app.id})"
                var msg = s"Application $appName is still in progress."
                logWarning(msg)
                msg = URLEncoder.encode(msg, "UTF-8")
                app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
                return false
            }

            val logInput = EventLoggingListener.openEventLog(new Path(eventLogFile), fs)
            val replayBus = new ReplayListenerBus()
            val ui = SparkUI.createHistoryUI(new SparkConf, replayBus, new SecurityManager(conf),
                appName + " (completed)", HistoryServer.UI_PATH_PREFIX + s"/${app.id}")
            try {
                replayBus.replay(logInput, eventLogFile)
            } finally {
                logInput.close()
            }
            appIdToUI(app.id) = ui
            webUi.attachSparkUI(ui)
            // Application UI is successfully rebuilt, so link the Master UI to it
            app.desc.appUiUrl = ui.basePath
            true
        } catch {
            case fnf: FileNotFoundException =>
                // Event logging is enabled for this application, but no event logs are found
                val title = s"Application history not found (${app.id})"
                var msg = s"No event logs found for application $appName in ${app.desc.eventLogDir}."
                logWarning(msg)
                msg += " Did you specify the correct logging directory?"
                msg = URLEncoder.encode(msg, "UTF-8")
                app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
                false
            case e: Exception =>
                // Relay exception message to application UI page
                val title = s"Application history load error (${app.id})"
                val exception = URLEncoder.encode(Utils.exceptionString(e), "UTF-8")
                var msg = s"Exception in replaying log for application $appName!"
                logError(msg, e)
                msg = URLEncoder.encode(msg, "UTF-8")
                app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&exception=$exception&title=$title"
                false
        }
    }

    /** Generate a new app ID given a app's submission date */
    /*
    * applicationID都是根据时间和application顺序号编码的
    * */
    def newApplicationId(submitDate: Date): String = {
        val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
        nextAppNumber += 1
        appId
    }

    /** Check for, and remove, any timed-out workers */
    /*
    * 遍历所有worker，判断超时，并删除
    * */
    def timeOutDeadWorkers() {
        // Copy the workers into an array so we don't modify the hashset while iterating through it
        val currentTime = System.currentTimeMillis()
        /*
        * 上一次心跳距离当前时间超过WORKER_TIMEOUT，则超时
        * */
        val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
        for (worker <- toRemove) {
            if (worker.state != WorkerState.DEAD) {
                logWarning("Removing %s because we got no heartbeat in %d seconds".format(
                    worker.id, WORKER_TIMEOUT / 1000))
                removeWorker(worker)
            } else {
                if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT)) {
                    workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
                }
            }
        }
    }

    def newDriverId(submitDate: Date): String = {
        val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
        nextDriverNumber += 1
        appId
    }

    /*
    * 所谓的createDriver或者createApplication等，都是构造一个DriverInfo、ApplicationInfo等类似的数据结构，保存起来而已
    * 启动Driver才是启动线程，DriverRunner这种
    * */
    def createDriver(desc: DriverDescription): DriverInfo = {
        val now = System.currentTimeMillis()
        val date = new Date(now)
        new DriverInfo(now, newDriverId(date), desc, date)
    }

    /*
    * 维护数据结构，然后给worker发消息，启动DriverRunner线程
    * */
    def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
        logInfo("Launching driver " + driver.id + " on worker " + worker.id)
        worker.addDriver(driver)
        driver.worker = Some(worker)
        worker.actor ! LaunchDriver(driver.id, driver.desc)
        driver.state = DriverState.RUNNING
    }

    def removeDriver(driverId: String, finalState: DriverState, exception: Option[Exception]) {
        drivers.find(d => d.id == driverId) match {
            case Some(driver) =>
                logInfo(s"Removing driver: $driverId")
                /*
                * 所谓的删除，也是维护数据结构
                * */
                drivers -= driver
                if (completedDrivers.size >= RETAINED_DRIVERS) {
                    val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
                    completedDrivers.trimStart(toRemove)
                }
                completedDrivers += driver
                persistenceEngine.removeDriver(driver)
                driver.state = finalState
                driver.exception = exception
                driver.worker.foreach(w => w.removeDriver(driver))
                /*
                * 而具体操作，都是在schedule()中实现的
                ***********************************************************************************************************
                * 该函数，先DriverRunner这个application对应的driver，然后spreadout分配该application对应的worker和executor *
                * *********************************************************************************************************
                * */
                schedule()
            case None =>
                logWarning(s"Asked to remove unknown driver: $driverId")
        }
    }
}

private[spark] object Master extends Logging {
    val systemName = "sparkMaster"
    private val actorName = "Master"

    /*
    * 主函数还是和worker一样，启动自己的actor，开始提供服务
    * */
    def main(argStrings: Array[String]) {
        SignalLogger.register(log)
        val conf = new SparkConf
        val args = new MasterArguments(argStrings, conf)
        val (actorSystem, _, _, _) = startSystemAndActor(args.host, args.port, args.webUiPort, conf)
        //长期运行，除非shutdown
        actorSystem.awaitTermination()
    }

    /**
      * Returns an `akka.tcp://...` URL for the Master actor given a sparkUrl `spark://host:port`.
      *
      * @throws SparkException if the url is invalid
      */
    def toAkkaUrl(sparkUrl: String, protocol: String): String = {
        val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
        AkkaUtils.address(protocol, systemName, host, port, actorName)
    }

    /**
      * Returns an akka `Address` for the Master actor given a sparkUrl `spark://host:port`.
      *
      * @throws SparkException if the url is invalid
      */
    def toAkkaAddress(sparkUrl: String, protocol: String): Address = {
        val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
        Address(protocol, systemName, host, port)
    }

    /**
      * Start the Master and return a four tuple of:
      * (1) The Master actor system
      * (2) The bound port
      * (3) The web UI bound port
      * (4) The REST server bound port, if any
      */
    def startSystemAndActor(
                                   host: String,
                                   port: Int,
                                   webUiPort: Int,
                                   conf: SparkConf): (ActorSystem, Int, Int, Option[Int]) = {
        val securityMgr = new SecurityManager(conf)
        /*
        * 说白了，就是调用了actor的ActorSystem
        * */
        val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port, conf = conf,
            securityManager = securityMgr)
        val actor = actorSystem.actorOf(
            Props(classOf[Master], host, boundPort, webUiPort, securityMgr, conf), actorName)
        val timeout = AkkaUtils.askTimeout(conf)
        val portsRequest = actor.ask(BoundPortsRequest)(timeout)
        val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
        (actorSystem, boundPort, portsResponse.webUIPort, portsResponse.restPort)
    }
}
