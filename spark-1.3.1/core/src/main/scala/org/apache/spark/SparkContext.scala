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

package org.apache.spark

import scala.language.implicitConversions

import java.io._
import java.lang.reflect.Constructor
import java.net.URI
import java.util.{Arrays, Properties, UUID}
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID.randomUUID

import scala.collection.{Map, Set}
import scala.collection.JavaConversions._
import scala.collection.generic.Growable
import scala.collection.mutable.HashMap
import scala.reflect.{ClassTag, classTag}

import akka.actor.Props

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{
    ArrayWritable, BooleanWritable, BytesWritable, DoubleWritable,
    FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable
}
import org.apache.hadoop.mapred.{
    FileInputFormat, InputFormat, JobConf, SequenceFileInputFormat,
    TextInputFormat
}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}

import org.apache.mesos.MesosNativeLibrary

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.{LocalSparkCluster, SparkHadoopUtil}
import org.apache.spark.executor.TriggerThreadDump
import org.apache.spark.input.{
    StreamInputFormat, PortableDataStream, WholeTextFileInputFormat,
    FixedLengthBinaryInputFormat
}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.partial.{ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{
    CoarseGrainedSchedulerBackend,
    SparkDeploySchedulerBackend, SimrSchedulerBackend
}
import org.apache.spark.scheduler.cluster.mesos.{CoarseMesosSchedulerBackend, MesosSchedulerBackend}
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.storage._
import org.apache.spark.ui.{SparkUI, ConsoleProgressBar}
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.util._

/**
  * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
  * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
  *
  * Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before
  * creating a new one.  This limitation may eventually be removed; see SPARK-2243 for more details.
  *
  * @param config a Spark Config object describing the application configuration. Any settings in
  *               this config overrides the default configs as well as system properties.
  */
/*
* 1、SparkContext是负责连接Spark cluster的；
* 2、SparkContext可以创建RDD、计算、广播变量到集群；
* 3、一个JVM只能有一个SparkContext，创建新的，就得停止旧的，但是可以设置参数允许多个SparkContext；
* 4、config对象配置了application配置，覆盖系统默认配置
* */
class SparkContext(config: SparkConf) extends Logging with ExecutorAllocationClient {

    // The call site where this SparkContext was constructed.
    private val creationSite: CallSite = Utils.getCallSite()

    // If true, log warnings instead of throwing exceptions when multiple SparkContexts are active
    /*
    *  获得是否允许多个SparkContext
    *  但是在SparkConf中没找到spark.driver.allowMultipleContexts的设置项
    * */
    private val allowMultipleContexts: Boolean =
    config.getBoolean("spark.driver.allowMultipleContexts", false)

    // In order to prevent multiple SparkContexts from being active at the same time, mark this
    // context as having started construction.
    // NOTE: this must be placed at the beginning of the SparkContext constructor.
    /*
    * 创建SparkContext首先设置这个，防止多个SparkContext启动
    * */
    SparkContext.markPartiallyConstructed(this, allowMultipleContexts)

    // This is used only by YARN for now, but should be relevant to other cluster types (Mesos,
    // etc) too. This is typically generated from InputFormatInfo.computePreferredLocations. It
    // contains a map from hostname to a list of input format splits on the host.
    /*
    * 在Yarn中才使用，选择启动container的node
    * */
    private[spark] var preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()

    val startTime = System.currentTimeMillis()

    //一种比sychronized关键字更轻量级的弱同步机制，每次调用返回的肯定是最新的值
    //该变量为true表示一个SparkContext已经stop了，不能再调用他的方法；false表示alive
    @volatile private var stopped: Boolean = false

    private def assertNotStopped(): Unit = {
        if (stopped) {
            throw new IllegalStateException("Cannot call methods on a stopped SparkContext")
        }
    }


    /** ***************************************以下是this()形式的SparkContext构造函数 ***********************************/

    /**
      * Create a SparkContext that loads settings from system properties (for instance, when
      * launching with ./bin/spark-submit).
      */
    /*用系统默认的SparkConf构造一个SparkContext
    * */
    def this() = this(new SparkConf())

    /**
      * :: DeveloperApi ::
      * Alternative constructor for setting preferred locations where Spark will create executors.
      * 可选构造器，设置container最近的Node节点
      *
      * @param preferredNodeLocationData used in YARN mode to select nodes to launch containers on.
      *                                  Can be generated using [[org.apache.spark.scheduler.InputFormatInfo.computePreferredLocations]]
      *                                  from a list of input files or InputFormats for the application.
      */
    @DeveloperApi
    def this(config: SparkConf, preferredNodeLocationData: Map[String, Set[SplitInfo]]) = {
        this(config)
        this.preferredNodeLocationData = preferredNodeLocationData
    }

    /**
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master  Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
      * @param appName A name for your application, to display on the cluster web UI
      * @param conf    a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
      */
    /*
    * 不通过SparkConf，通过各个sparkConf属性（master、appName）直接构造SparkContext
    * 其实内部调用的还是SparkConf
    * */
    def this(master: String, appName: String, conf: SparkConf) =
    this(SparkContext.updatedConf(conf, master, appName))

    /**
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master      Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
      * @param appName     A name for your application, to display on the cluster web UI.
      * @param sparkHome   Location where Spark is installed on cluster nodes.
      * @param jars        Collection of JARs to send to the cluster. These can be paths on the local file
      *                    system or HDFS, HTTP, HTTPS, or FTP URLs.////////////////////////////////////可以是HDFS等云盘路径
      * @param environment Environment variables to set on worker nodes./////////////worker节点上设置的环境变量，比如什么呢？？？
      */
    /*
    * 其实内部还是调用的sparkConf
    * Jar包路径可以HDFS、HTTPS、FTP的URLs，意味着可以远程服务器上下载这些依赖，在远程服务器上专门构建一个配置中心这样部署我们的spark架构
    * */
    def this(
                    master: String,
                    appName: String,
                    sparkHome: String = null,
                    jars: Seq[String] = Nil,
                    environment: Map[String, String] = Map(),
                    preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()) = {
        this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
        this.preferredNodeLocationData = preferredNodeLocationData
    }

    // NOTE: The below constructors could be consolidated using default arguments. Due to
    // Scala bug SI-8479, however, this causes the compile step to fail when generating docs.
    // Until we have a good workaround for that bug the constructors remain broken out.

    /**
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master  Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
      * @param appName A name for your application, to display on the cluster web UI.
      */
    private[spark] def this(master: String, appName: String) =
        this(master, appName, null, Nil, Map(), Map())

    /**
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master    Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
      * @param appName   A name for your application, to display on the cluster web UI.
      * @param sparkHome Location where Spark is installed on cluster nodes.
      */
    private[spark] def this(master: String, appName: String, sparkHome: String) =
        this(master, appName, sparkHome, Nil, Map(), Map())

    /**
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master    Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
      * @param appName   A name for your application, to display on the cluster web UI.
      * @param sparkHome Location where Spark is installed on cluster nodes.
      * @param jars      Collection of JARs to send to the cluster. These can be paths on the local file
      *                  system or HDFS, HTTP, HTTPS, or FTP URLs.
      */
    private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
        this(master, appName, sparkHome, jars, Map(), Map())

    /** ***************************************以上this()形式的SparkContext构造函数结束 ***********************************/


    // log out Spark Version in Spark driver log
    /*
    * $SPARK_VERSION是如何引用的？？？？？？
    * */
    logInfo(s"Running Spark version $SPARK_VERSION")

    /*
    * 防止破坏原先的SparkConf，所以先clone
    * */
    //这是定义变量，不是定义函数，只要当下执行的，def才是函数、方法，别搞混了
    //val =的 是唯一执行的
    private[spark] val conf = config.clone()
    //检查conf是否设置合法
    conf.validateSettings()

    /**
      * Return a copy of this SparkContext's configuration. The configuration ''cannot'' be
      * changed at runtime.
      */
    /*
    *
    * 可以在运行过程中，修改SparkConf并生效
    * */
    def getConf: SparkConf = conf.clone()

    /*
    *
    * 以下几个if都是异常判断
    * */
    if (!conf.contains("spark.master")) {
        throw new SparkException("A master URL must be set in your configuration")
    }
    if (!conf.contains("spark.app.name")) {
        throw new SparkException("An application name must be set in your configuration")
    }

    if (conf.getBoolean("spark.logConf", false)) {
        logInfo("Spark configuration:\n" + conf.toDebugString)
    }

    // Set Spark driver host and port system properties
    conf.setIfMissing("spark.driver.host", Utils.localHostName())
    conf.setIfMissing("spark.driver.port", "0")

    /*
    * 以下操作都是对SparkConf，get一些key-value数据
    * */
    val jars: Seq[String] =
        conf.getOption("spark.jars").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

    //读取外部配置文件
    val files: Seq[String] =
        conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

    val master = conf.get("spark.master")
    val appName = conf.get("spark.app.name")

    /*
    * 下面两个函数，如果在写监控程序的时候，应该是需要开启的！！！
    * */
    //是否记录 Spark事件，用于 应用程序在完成后 重构 webUI。
    private[spark] val isEventLogEnabled = conf.getBoolean("spark.eventLog.enabled", false)
    //当spark.eventLog.enabled=true时，才能生效，为每个application创建目录，
    // 将application的时间记录在该目录里，可以设置为HDFS目录，以便history server读取历史记录文件
    private[spark] val eventLogDir: Option[URI] = {
        if (isEventLogEnabled) {
            val unresolvedDir = conf.get("spark.eventLog.dir", EventLoggingListener.DEFAULT_LOG_DIR)
                    .stripSuffix("/")
            Some(Utils.resolveURI(unresolvedDir))
        } else {
            None
        }
    }
    /*
    * 是否压缩记录Spark事件，前提spark.eventLog.enabled为true。
    * */
    private[spark] val eventLogCodec: Option[String] = {
        //getBoolean后面的第二个参数false是默认值。都可以用SparkConf.set(key, value)设置的
        val compress = conf.getBoolean("spark.eventLog.compress", false)
        if (compress && isEventLogEnabled) {
            Some(CompressionCodec.getCodecName(conf)).map(CompressionCodec.getShortName)
        } else {
            None
        }
    }

    // Generate the random name for a temp folder in Tachyon
    // Add a timestamp as the suffx here to make it more safe
    val tachyonFolderName = "spark-" + randomUUID.toString()
    conf.set("spark.tachyonStore.folderName", tachyonFolderName)

    val isLocal = (master == "local" || master.startsWith("local["))

    if (master == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")

    // An asynchronous listener bus for Spark events
    /*
    *
    * 1、异步总线
    * Spark定义了一个trait的ListenerBus，可以接收事件并将事件提交给对应的事件监听器
    * 即所有spark消息SparkListenerEvents 被异步的发送给已经注册过的SparkListeners.
    * 在SparkContext中, 首先会创建LiveListenerBus实例,这个类主要功能如下:
    * 1、保存有消息队列,负责消息的缓存
    * 2、保存有注册过的listener,负责消息的分发
    * */

    /*
    * ListenerBus机制需要了解一下！
    * */
    private[spark] val listenerBus = new LiveListenerBus

    conf.set("spark.executor.id", SparkContext.DRIVER_IDENTIFIER)

    // Create the Spark execution environment (cache, map output tracker, etc)
    /*
    *
    * 以下就是定义并创建spark的执行环境了
    *
    * */
    // This function allows components created by SparkEnv to be mocked in unit tests:
    /*
    * 是模拟一个SparkEnv环境用来测试么？
    *
    * */
    private[spark] def createSparkEnv(
                                             conf: SparkConf,
                                             isLocal: Boolean,
                                             listenerBus: LiveListenerBus): SparkEnv = {
        /*
        * SparkEnv可以根据传入的isDriver创建driver或executor的SparkEnv及内部的actor
        * */
        SparkEnv.createDriverEnv(conf, isLocal, listenerBus)
    }


    /*
    * 2、通过conf、listenerBus创建sparkEnv
    *
    * SparkEnv.createDriverEnv->SparkEnv.creat(isDriver=true)（注：有两种SparkEnv：1、driver SparkEnv；2、executor SparkEnv）
    * */
    private[spark] val env = createSparkEnv(conf, isLocal, listenerBus)
    //之所以可以直接用SparkEnv.set，是因为这是SparkEnv class的伴生对象
    SparkEnv.set(env)

    // Used to store a URL for each static file/jar together with the file's local timestamp
    private[spark] val addedFiles = HashMap[String, Long]()
    private[spark] val addedJars = HashMap[String, Long]()

    // Keeps track of all persisted RDDs
    /*
    * SparkContext可以直接查看当前持久化的RDD信息
    * */
    private[spark] val persistentRdds = new TimeStampedWeakValueHashMap[Int, RDD[_]]
    private[spark] val metadataCleaner =
        new MetadataCleaner(MetadataCleanerType.SPARK_CONTEXT, this.cleanup, conf)

    /*
    * 3、添加总线的各种UI相关监控器
    * JobProgressListener类是Spark的ListenerBus中一个很重要的监听器，可以用于记录Spark任务的Job和Stage等信息，
    * 比如在Spark UI页面上Job和Stage运行状况以及运行进度的显示等数据，就是从JobProgressListener中获得的。
    * 各个监听器都需要添加到总线中
    */
    private[spark] val jobProgressListener = new JobProgressListener(conf)
    listenerBus.addListener(jobProgressListener)

    /*
    * spark状态跟踪器
    * 监控Job和Stage，使用JobProgressListener的监控数据，做了一些加工
    * 属于UI部分
    * */
    val statusTracker = new SparkStatusTracker(this)

    /*
    * 控制台进度条
    * ConsoleProgressBar负责将SparkStatusTracker提供的数据打印到控制台上。
    * 属于UI部分
    * */
    private[spark] val progressBar: Option[ConsoleProgressBar] =
        if (conf.getBoolean("spark.ui.showConsoleProgress", true) && !log.isInfoEnabled) {
            Some(new ConsoleProgressBar(this))
        } else {
            None
        }

    /** ****************************UI相关 ************************************/
    // Initialize the Spark UI，初始化UI
    private[spark] val ui: Option[SparkUI] =
    if (conf.getBoolean("spark.ui.enabled", true)) {
        Some(SparkUI.createLiveUI(this, conf, listenerBus, jobProgressListener,
            env.securityManager, appName))
    } else {
        // For tests, do not enable the UI
        //测试的话，不要打开UI
        None
    }

    // Bind the UI before starting the task scheduler to communicate
    // the bound port to the cluster manager properly
    //绑定UI端口，使其在task scheduler与CM通信之前可以访问
    ui.foreach(_.bind())

    /**
      * A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse.
      *
      * '''Note:''' As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
      * plan to set some global configurations for all Hadoop RDDs.
      */
    /*
    * Hadoop配置
    * */
    val hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(conf)

    // Add each JAR given through the constructor
    /*
    * 4、根据SparkContext构造器里的jar参数，下载对应的Jar包到各个worker
    * */
    if (jars != null) {
        /*
        * 通过env的http服务下载对应目录的jar包
        * env.httpFileServer.addJar(new File(path))
        * */
        jars.foreach(addJar)
    }

    /*
    * 5、下载File配置文件
    * */
    if (files != null) {
        files.foreach(addFile)
    }

    private def warnSparkMem(value: String): String = {
        logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
                "deprecated, please use spark.executor.memory instead.")
        value
    }


    /*
    * 6、获取executor内存信息
    * */
    private[spark] val executorMemory = conf.getOption("spark.executor.memory")
            .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
            .orElse(Option(System.getenv("SPARK_MEM")).map(warnSparkMem))
            .map(Utils.memoryStringToMb)
            .getOrElse(512)

    // Environment variables to pass to our executors.
    private[spark] val executorEnvs = HashMap[String, String]()

    // Convert java options to env vars as a work around
    // since we can't set env vars directly in sbt.
    for {(envKey, propKey) <- Seq(("SPARK_TESTING", "spark.testing"))
         value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} {
        executorEnvs(envKey) = value
    }
    Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
        executorEnvs("SPARK_PREPEND_CLASSES") = v
    }
    // The Mesos scheduler backend relies on this environment variable to set executor memory.
    // TODO: Set this only in the Mesos scheduler.
    executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
    executorEnvs ++= conf.getExecutorEnv

    // Set SPARK_USER for user who is running SparkContext.
    val sparkUser = Utils.getCurrentUserName()
    executorEnvs("SPARK_USER") = sparkUser


    // Create and start the scheduler
    /*
    *
    * 7、此处开始创建，但是并没有启动TaskScheduler和backend
    * 首先创建TaskScheduler，然后TaskScheduler初始化了scheduleBackend，构建调度器schedulableBuilder（FIFO、FAIR）
    *
    * */
    private[spark] var (schedulerBackend, taskScheduler) =
    SparkContext.createTaskScheduler(this, master)


    /*
    * 拿到该SparkContext中启动的SparkEnv中的actorSystem，启动心跳检测actor
    * */
    private val heartbeatReceiver = env.actorSystem.actorOf(
        Props(new HeartbeatReceiver(taskScheduler)), "HeartbeatReceiver")


    /*
    *
    *
    * 8、创建DAGScheduler对象
    * 用到了TaskScheduler句柄
    * */
    @volatile private[spark] var dagScheduler: DAGScheduler = _
    try {
        /*
        *
        * this用到了TaskScheduler句柄，因为这个this调用了SparkContext，并且通过SparkContext拿到了对应的TaskScheduler作为参数，构建了DAGScheduler，但没有启动DAGScheduler
        * */
        dagScheduler = new DAGScheduler(this)
    } catch {
        case e: Exception => {
            try {
                stop()
            } finally {
                throw new SparkException("Error while constructing DAGScheduler", e)
            }
        }
    }


    // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
    // constructor
    /*
    * 9、启动TaskScheduler
    * 因为DAGScheduler创建需要TaskScheduler，所以先启动TaskScheduler
    * 首先启动backend.start()
    *
    * TaskScheduler.start()->
    * backend.start() + 开启推测任务执行->
    * 启动backend的actor，SparkEnv.get.actorSystem.actorOf->
    * 接受ReviveOffers信息调用reviveOffers函数->
    * executor.launchTask()->
    * tr = TaskRunner(taskid)->
    * threadPool.execute(tr)
    * */
    taskScheduler.start()
    /*
    *
    *
    * NOTE： DAGScheduler为什么没有start()方法？因为DAGScheduler只有runJob方法，只有调用方法，不需要启动，
    * 他只需要在提交Action时，调用DAGScheduler.runJob，进而调用TaskScheduler等的操作即可；
    * 在SparkContext初始化阶段，只需要创建对象，启动TaskScheduler和backend的start即可。
    *
    *
    *
    * */


    /*
    *
    * TaskScheduler.applicationId()->backend.applicationId()
    * */
    val applicationId: String = taskScheduler.applicationId()
    conf.set("spark.app.id", applicationId)

    /*
    * 10、初始化BlockManager
    * */
    env.blockManager.initialize(applicationId)

    /*
    *
    * 11、监控系统启动
    * */
    val metricsSystem = env.metricsSystem

    // The metrics system for Driver need to be set spark.app.id to app ID.
    // So it should start after we get app ID from the task scheduler and set spark.app.id.
    metricsSystem.start()
    // Attach the driver metrics servlet handler to the web ui after the metrics system is started.
    metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))

    // Optionally log Spark events
    /*
    * 向总线注册日志事件
    * */
    private[spark] val eventLogger: Option[EventLoggingListener] = {
        if (isEventLogEnabled) {
            val logger =
                new EventLoggingListener(applicationId, eventLogDir.get, conf, hadoopConfiguration)
            logger.start()
            listenerBus.addListener(logger)
            Some(logger)
        } else None
    }

    // Optionally scale number of executors dynamically based on workload. Exposed for testing.
    /*
    * 动态分配资源，仅仅yarn模式下可以用
    * */
    private val dynamicAllocationEnabled = conf.getBoolean("spark.dynamicAllocation.enabled", false)
    private val dynamicAllocationTesting = conf.getBoolean("spark.dynamicAllocation.testing", false)
    private[spark] val executorAllocationManager: Option[ExecutorAllocationManager] =
        if (dynamicAllocationEnabled) {
            assert(master.contains("yarn") || dynamicAllocationTesting,
                "Dynamic allocation of executors is currently only supported in YARN mode")
            Some(new ExecutorAllocationManager(this, listenerBus, conf))
        } else {
            None
        }
    executorAllocationManager.foreach(_.start())

    private[spark] val cleaner: Option[ContextCleaner] = {
        if (conf.getBoolean("spark.cleaner.referenceTracking", true)) {
            Some(new ContextCleaner(this))
        } else {
            None
        }
    }
    cleaner.foreach(_.start())

    setupAndStartListenerBus()
    postEnvironmentUpdate()
    postApplicationStart()

    private[spark] var checkpointDir: Option[String] = None

    // Thread Local variable that can be used by users to pass information down the stack
    private val localProperties = new InheritableThreadLocal[Properties] {
        override protected def childValue(parent: Properties): Properties = new Properties(parent)

        override protected def initialValue(): Properties = new Properties()
    }

    /**
      * Called by the web UI to obtain executor thread dumps.  This method may be expensive.
      * Logs an error and returns None if we failed to obtain a thread dump, which could occur due
      * to an executor being dead or unresponsive or due to network issues while sending the thread
      * dump message back to the driver.
      */
    private[spark] def getExecutorThreadDump(executorId: String): Option[Array[ThreadStackTrace]] = {
        try {
            if (executorId == SparkContext.DRIVER_IDENTIFIER) {
                Some(Utils.getThreadDump())
            } else {
                val (host, port) = env.blockManager.master.getActorSystemHostPortForExecutor(executorId).get
                val actorRef = AkkaUtils.makeExecutorRef("ExecutorActor", conf, host, port, env.actorSystem)
                Some(AkkaUtils.askWithReply[Array[ThreadStackTrace]](TriggerThreadDump, actorRef,
                    AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), AkkaUtils.askTimeout(conf)))
            }
        } catch {
            case e: Exception =>
                logError(s"Exception getting thread dump from executor $executorId", e)
                None
        }
    }

    private[spark] def getLocalProperties: Properties = localProperties.get()

    private[spark] def setLocalProperties(props: Properties) {
        localProperties.set(props)
    }

    @deprecated("Properties no longer need to be explicitly initialized.", "1.0.0")
    def initLocalProperties() {
        localProperties.set(new Properties())
    }

    /**
      * Set a local property that affects jobs submitted from this thread, such as the
      * Spark fair scheduler pool.
      */
    def setLocalProperty(key: String, value: String) {
        if (value == null) {
            localProperties.get.remove(key)
        } else {
            localProperties.get.setProperty(key, value)
        }
    }

    /**
      * Get a local property set in this thread, or null if it is missing. See
      * [[org.apache.spark.SparkContext.setLocalProperty]].
      */
    def getLocalProperty(key: String): String =
        Option(localProperties.get).map(_.getProperty(key)).getOrElse(null)

    /** Set a human readable description of the current job. */
    def setJobDescription(value: String) {
        setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, value)
    }

    /**
      * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
      * different value or cleared.
      *
      * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
      * Application programmers can use this method to group all those jobs together and give a
      * group description. Once set, the Spark web UI will associate such jobs with this group.
      *
      * The application can also use [[org.apache.spark.SparkContext.cancelJobGroup]] to cancel all
      * running jobs in this group. For example,
      * {{{
      * // In the main thread:
      * sc.setJobGroup("some_job_to_cancel", "some job description")
      * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
      *
      * // In a separate thread:
      * sc.cancelJobGroup("some_job_to_cancel")
      * }}}
      *
      * If interruptOnCancel is set to true for the job group, then job cancellation will result
      * in Thread.interrupt() being called on the job's executor threads. This is useful to help ensure
      * that the tasks are actually stopped in a timely manner, but is off by default due to HDFS-1208,
      * where HDFS may respond to Thread.interrupt() by marking nodes as dead.
      */
    /*
    *
    * 使用一个SparkContext时，可以针对不同搞得Job进行分组提交和取消
    * */
    def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean = false) {
        setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
        setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
        // Note: Specifying interruptOnCancel in setJobGroup (rather than cancelJobGroup) avoids
        // changing several public APIs and allows Spark cancellations outside of the cancelJobGroup
        // APIs to also take advantage of this property (e.g., internal job failures or canceling from
        // JobProgressTab UI) on a per-job basis.
        setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
    }

    /** Clear the current thread's job group ID and its description. */
    def clearJobGroup() {
        setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
        setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
        setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
    }

    /*
    *
    * 监控
    * */
    // Post init
    taskScheduler.postStartHook()

    private val dagSchedulerSource = new DAGSchedulerSource(this.dagScheduler)
    private val blockManagerSource = new BlockManagerSource(SparkEnv.get.blockManager)

    private def initDriverMetrics() {
        SparkEnv.get.metricsSystem.registerSource(dagSchedulerSource)
        SparkEnv.get.metricsSystem.registerSource(blockManagerSource)
    }

    initDriverMetrics()

    // Methods for creating RDDs

    /** Distribute a local Scala collection to form an RDD.
      *
      * @note Parallelize acts lazily. If `seq` is a mutable collection and is altered after the call
      *       to parallelize and before the first action on the RDD, the resultant RDD will reflect the
      *       modified collection. Pass a copy of the argument to avoid this.
      */
    def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
        assertNotStopped()
        new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
    }

    /** Distribute a local Scala collection to form an RDD.
      *
      * This method is identical to `parallelize`.
      */
    def makeRDD[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
        parallelize(seq, numSlices)
    }

    /** Distribute a local Scala collection to form an RDD, with one or more
      * location preferences (hostnames of Spark nodes) for each object.
      * Create a new partition for each collection item. */
    def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = {
        assertNotStopped()
        val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2)).toMap
        new ParallelCollectionRDD[T](this, seq.map(_._1), seq.size, indexToPrefs)
    }

    /**
      * Read a text file from HDFS, a local file system (available on all nodes), or any
      * Hadoop-supported file system URI, and return it as an RDD of Strings.
      */
    /*
    *
    * textFile和wholeTextFiles的区别：
    * 都可以读取目录下的所有文件，但是
    * 1、textFile形成的RDD是文件的每一行；
    * 2、wholeTextFiles形成的RDD是每个文件，格式为key-valu(key:文件路径；value:文件整体内容)
    *
    * */
    def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String] = {
        assertNotStopped()
        hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
            minPartitions).map(pair => pair._2.toString).setName(path)
    }

    /**
      * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
      * Hadoop-supported file system URI. Each file is read as a single record and returned in a
      * key-value pair, where the key is the path of each file, the value is the content of each file.
      *
      * <p> For example, if you have the following files:
      * {{{
      *   hdfs://a-hdfs-path/part-00000
      *   hdfs://a-hdfs-path/part-00001
      *   ...
      *   hdfs://a-hdfs-path/part-nnnnn
      * }}}
      *
      * Do `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`,
      *
      * <p> then `rdd` contains
      * {{{
      *   (a-hdfs-path/part-00000, its content)
      *   (a-hdfs-path/part-00001, its content)
      *   ...
      *   (a-hdfs-path/part-nnnnn, its content)
      * }}}
      *
      * @note Small files are preferred, large file is also allowable, but may cause bad performance.
      * @param minPartitions A suggestion value of the minimal splitting number for input data.
      */
    def wholeTextFiles(path: String, minPartitions: Int = defaultMinPartitions):
    RDD[(String, String)] = {
        assertNotStopped()
        val job = new NewHadoopJob(hadoopConfiguration)
        NewFileInputFormat.addInputPath(job, new Path(path))
        val updateConf = job.getConfiguration
        new WholeTextFileRDD(
            this,
            classOf[WholeTextFileInputFormat],
            classOf[String],
            classOf[String],
            updateConf,
            minPartitions).setName(path)
    }


    /**
      * :: Experimental ::
      *
      * Get an RDD for a Hadoop-readable dataset as PortableDataStream for each file
      * (useful for binary data)
      *
      * For example, if you have the following files:
      * {{{
      *   hdfs://a-hdfs-path/part-00000
      *   hdfs://a-hdfs-path/part-00001
      *   ...
      *   hdfs://a-hdfs-path/part-nnnnn
      * }}}
      *
      * Do
      * `val rdd = sparkContext.dataStreamFiles("hdfs://a-hdfs-path")`,
      *
      * then `rdd` contains
      * {{{
      *   (a-hdfs-path/part-00000, its content)
      *   (a-hdfs-path/part-00001, its content)
      *   ...
      *   (a-hdfs-path/part-nnnnn, its content)
      * }}}
      *
      * @param minPartitions A suggestion value of the minimal splitting number for input data.
      * @note Small files are preferred; very large files may cause bad performance.
      */
    @Experimental
    def binaryFiles(path: String, minPartitions: Int = defaultMinPartitions):
    RDD[(String, PortableDataStream)] = {
        assertNotStopped()
        val job = new NewHadoopJob(hadoopConfiguration)
        NewFileInputFormat.addInputPath(job, new Path(path))
        val updateConf = job.getConfiguration
        new BinaryFileRDD(
            this,
            classOf[StreamInputFormat],
            classOf[String],
            classOf[PortableDataStream],
            updateConf,
            minPartitions).setName(path)
    }

    /**
      * :: Experimental ::
      *
      * Load data from a flat binary file, assuming the length of each record is constant.
      *
      * '''Note:''' We ensure that the byte array for each record in the resulting RDD
      * has the provided record length.
      *
      * @param path         Directory to the input data files
      * @param recordLength The length at which to split the records
      * @return An RDD of data with values, represented as byte arrays
      */
    @Experimental
    def binaryRecords(path: String, recordLength: Int, conf: Configuration = hadoopConfiguration)
    : RDD[Array[Byte]] = {
        assertNotStopped()
        conf.setInt(FixedLengthBinaryInputFormat.RECORD_LENGTH_PROPERTY, recordLength)
        val br = newAPIHadoopFile[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](path,
            classOf[FixedLengthBinaryInputFormat],
            classOf[LongWritable],
            classOf[BytesWritable],
            conf = conf)
        val data = br.map { case (k, v) =>
            val bytes = v.getBytes
            assert(bytes.length == recordLength, "Byte array does not have correct length")
            bytes
        }
        data
    }

    /**
      * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf given its InputFormat and other
      * necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable),
      * using the older MapReduce API (`org.apache.hadoop.mapred`).
      *
      * @param conf             JobConf for setting up the dataset. Note: This will be put into a Broadcast.
      *                         Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
      *                         sure you won't modify the conf. A safe approach is always creating a new conf for
      *                         a new RDD.
      * @param inputFormatClass Class of the InputFormat
      * @param keyClass         Class of the keys
      * @param valueClass       Class of the values
      * @param minPartitions    Minimum number of Hadoop Splits to generate.
      *
      *                         '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
      *                         record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *                         operation will create many references to the same object.
      *                         If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *                         copy them using a `map` function.
      */
    def hadoopRDD[K, V](
                               conf: JobConf,
                               inputFormatClass: Class[_ <: InputFormat[K, V]],
                               keyClass: Class[K],
                               valueClass: Class[V],
                               minPartitions: Int = defaultMinPartitions
                       ): RDD[(K, V)] = {
        assertNotStopped()
        // Add necessary security credentials to the JobConf before broadcasting it.
        SparkHadoopUtil.get.addCredentials(conf)
        new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minPartitions)
    }

    /** Get an RDD for a Hadoop file with an arbitrary InputFormat
      *
      * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
      * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      * operation will create many references to the same object.
      * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      * copy them using a `map` function.
      */
    def hadoopFile[K, V](
                                path: String,
                                inputFormatClass: Class[_ <: InputFormat[K, V]],
                                keyClass: Class[K],
                                valueClass: Class[V],
                                minPartitions: Int = defaultMinPartitions
                        ): RDD[(K, V)] = {
        assertNotStopped()
        // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
        val confBroadcast = broadcast(new SerializableWritable(hadoopConfiguration))
        val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
        new HadoopRDD(
            this,
            confBroadcast,
            Some(setInputPathsFunc),
            inputFormatClass,
            keyClass,
            valueClass,
            minPartitions).setName(path)
    }

    /**
      * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
      * values and the InputFormat so that users don't need to pass them directly. Instead, callers
      * can just write, for example,
      * {{{
      * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
      * }}}
      *
      * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
      * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      * operation will create many references to the same object.
      * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      * copy them using a `map` function.
      */
    def hadoopFile[K, V, F <: InputFormat[K, V]]
    (path: String, minPartitions: Int)
    (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = {
        hadoopFile(path,
            fm.runtimeClass.asInstanceOf[Class[F]],
            km.runtimeClass.asInstanceOf[Class[K]],
            vm.runtimeClass.asInstanceOf[Class[V]],
            minPartitions)
    }

    /**
      * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
      * values and the InputFormat so that users don't need to pass them directly. Instead, callers
      * can just write, for example,
      * {{{
      * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
      * }}}
      *
      * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
      * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      * operation will create many references to the same object.
      * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      * copy them using a `map` function.
      */
    def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
                                                (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] =
        hadoopFile[K, V, F](path, defaultMinPartitions)

    /** Get an RDD for a Hadoop file with an arbitrary new API InputFormat. */
    def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]]
    (path: String)
    (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = {
        newAPIHadoopFile(
            path,
            fm.runtimeClass.asInstanceOf[Class[F]],
            km.runtimeClass.asInstanceOf[Class[K]],
            vm.runtimeClass.asInstanceOf[Class[V]])
    }

    /**
      * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
      * and extra configuration options to pass to the input format.
      *
      * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
      * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      * operation will create many references to the same object.
      * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      * copy them using a `map` function.
      */
    def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
                                                                 path: String,
                                                                 fClass: Class[F],
                                                                 kClass: Class[K],
                                                                 vClass: Class[V],
                                                                 conf: Configuration = hadoopConfiguration): RDD[(K, V)] = {
        assertNotStopped()
        // The call to new NewHadoopJob automatically adds security credentials to conf,
        // so we don't need to explicitly add them ourselves
        val job = new NewHadoopJob(conf)
        NewFileInputFormat.addInputPath(job, new Path(path))
        val updatedConf = job.getConfiguration
        new NewHadoopRDD(this, fClass, kClass, vClass, updatedConf).setName(path)
    }

    /**
      * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
      * and extra configuration options to pass to the input format.
      *
      * @param conf   Configuration for setting up the dataset. Note: This will be put into a Broadcast.
      *               Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
      *               sure you won't modify the conf. A safe approach is always creating a new conf for
      *               a new RDD.
      * @param fClass Class of the InputFormat
      * @param kClass Class of the keys
      * @param vClass Class of the values
      *
      *               '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
      *               record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *               operation will create many references to the same object.
      *               If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *               copy them using a `map` function.
      */
    def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
                                                                conf: Configuration = hadoopConfiguration,
                                                                fClass: Class[F],
                                                                kClass: Class[K],
                                                                vClass: Class[V]): RDD[(K, V)] = {
        assertNotStopped()
        // Add necessary security credentials to the JobConf. Required to access secure HDFS.
        val jconf = new JobConf(conf)
        SparkHadoopUtil.get.addCredentials(jconf)
        new NewHadoopRDD(this, fClass, kClass, vClass, jconf)
    }

    /** Get an RDD for a Hadoop SequenceFile with given key and value types.
      *
      * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
      * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      * operation will create many references to the same object.
      * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      * copy them using a `map` function.
      */
    def sequenceFile[K, V](path: String,
                           keyClass: Class[K],
                           valueClass: Class[V],
                           minPartitions: Int
                          ): RDD[(K, V)] = {
        assertNotStopped()
        val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
        hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
    }

    /** Get an RDD for a Hadoop SequenceFile with given key and value types.
      *
      * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
      * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      * operation will create many references to the same object.
      * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      * copy them using a `map` function.
      * */
    def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]): RDD[(K, V)] = {
        assertNotStopped()
        sequenceFile(path, keyClass, valueClass, defaultMinPartitions)
    }

    /**
      * Version of sequenceFile() for types implicitly convertible to Writables through a
      * WritableConverter. For example, to access a SequenceFile where the keys are Text and the
      * values are IntWritable, you could simply write
      * {{{
      * sparkContext.sequenceFile[String, Int](path, ...)
      * }}}
      *
      * WritableConverters are provided in a somewhat strange way (by an implicit function) to support
      * both subclasses of Writable and types for which we define a converter (e.g. Int to
      * IntWritable). The most natural thing would've been to have implicit objects for the
      * converters, but then we couldn't have an object for every subclass of Writable (you can't
      * have a parameterized singleton object). We use functions instead to create a new converter
      * for the appropriate type. In addition, we pass the converter a ClassTag of its type to
      * allow it to figure out the Writable class to use in the subclass case.
      *
      * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
      * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      * operation will create many references to the same object.
      * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      * copy them using a `map` function.
      */
    def sequenceFile[K, V]
    (path: String, minPartitions: Int = defaultMinPartitions)
    (implicit km: ClassTag[K], vm: ClassTag[V],
     kcf: () => WritableConverter[K], vcf: () => WritableConverter[V])
    : RDD[(K, V)] = {
        assertNotStopped()
        val kc = kcf()
        val vc = vcf()
        val format = classOf[SequenceFileInputFormat[Writable, Writable]]
        val writables = hadoopFile(path, format,
            kc.writableClass(km).asInstanceOf[Class[Writable]],
            vc.writableClass(vm).asInstanceOf[Class[Writable]], minPartitions)
        writables.map { case (k, v) => (kc.convert(k), vc.convert(v)) }
    }

    /**
      * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
      * BytesWritable values that contain a serialized partition. This is still an experimental
      * storage format and may not be supported exactly as is in future Spark releases. It will also
      * be pretty slow if you use the default serializer (Java serialization),
      * though the nice thing about it is that there's very little effort required to save arbitrary
      * objects.
      */
    def objectFile[T: ClassTag](
                                       path: String,
                                       minPartitions: Int = defaultMinPartitions
                               ): RDD[T] = {
        assertNotStopped()
        sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
                .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes, Utils.getContextOrSparkClassLoader))
    }

    protected[spark] def checkpointFile[T: ClassTag](
                                                            path: String
                                                    ): RDD[T] = {
        new CheckpointRDD[T](this, path)
    }

    /** Build the union of a list of RDDs. */
    def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = {
        val partitioners = rdds.flatMap(_.partitioner).toSet
        if (partitioners.size == 1) {
            new PartitionerAwareUnionRDD(this, rdds)
        } else {
            new UnionRDD(this, rdds)
        }
    }

    /** Build the union of a list of RDDs passed as variable-length arguments. */
    def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] =
        union(Seq(first) ++ rest)

    /** Get an RDD that has no partitions or elements. */
    def emptyRDD[T: ClassTag] = new EmptyRDD[T](this)

    // Methods for creating shared variables

    /**
      * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
      * values to using the `+=` method. Only the driver can access the accumulator's `value`.
      */
    def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]) =
        new Accumulator(initialValue, param)

    /**
      * Create an [[org.apache.spark.Accumulator]] variable of a given type, with a name for display
      * in the Spark UI. Tasks can "add" values to the accumulator using the `+=` method. Only the
      * driver can access the accumulator's `value`.
      */
    def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T]) = {
        new Accumulator(initialValue, param, Some(name))
    }

    /**
      * Create an [[org.apache.spark.Accumulable]] shared variable, to which tasks can add values
      * with `+=`. Only the driver can access the accumuable's `value`.
      *
      * @tparam R accumulator result type
      * @tparam T type that can be added to the accumulator
      */
    def accumulable[R, T](initialValue: R)(implicit param: AccumulableParam[R, T]) =
        new Accumulable(initialValue, param)

    /**
      * Create an [[org.apache.spark.Accumulable]] shared variable, with a name for display in the
      * Spark UI. Tasks can add values to the accumuable using the `+=` operator. Only the driver can
      * access the accumuable's `value`.
      *
      * @tparam R accumulator result type
      * @tparam T type that can be added to the accumulator
      */
    def accumulable[R, T](initialValue: R, name: String)(implicit param: AccumulableParam[R, T]) =
        new Accumulable(initialValue, param, Some(name))

    /**
      * Create an accumulator from a "mutable collection" type.
      *
      * Growable and TraversableOnce are the standard APIs that guarantee += and ++=, implemented by
      * standard mutable collections. So you can use this with mutable Map, Set, etc.
      */
    def accumulableCollection[R <% Growable[T] with TraversableOnce[T] with Serializable : ClassTag, T]
    (initialValue: R): Accumulable[R, T] = {
        val param = new GrowableAccumulableParam[R, T]
        new Accumulable(initialValue, param)
    }

    /**
      * Broadcast a read-only variable to the cluster, returning a
      * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
      * The variable will be sent to each cluster only once.
      */
    /*
    *
    * 通过调用env.broadcastManager广播
    * */
    def broadcast[T: ClassTag](value: T): Broadcast[T] = {
        assertNotStopped()
        if (classOf[RDD[_]].isAssignableFrom(classTag[T].runtimeClass)) {
            // This is a warning instead of an exception in order to avoid breaking user programs that
            // might have created RDD broadcast variables but not used them:
            logWarning("Can not directly broadcast RDDs; instead, call collect() and "
                    + "broadcast the result (see SPARK-5063)")
        }
        val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
        val callSite = getCallSite
        logInfo("Created broadcast " + bc.id + " from " + callSite.shortForm)
        cleaner.foreach(_.registerBroadcastForCleanup(bc))
        bc
    }

    /**
      * Add a file to be downloaded with this Spark job on every node.
      * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
      * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
      * use `SparkFiles.get(fileName)` to find its download location.
      */
    /*
    * 调用本地文件或hadoop支持操作下载远程文件
    * Add a file to be downloaded with this Spark job on every node.
    * */
    def addFile(path: String): Unit = {
        addFile(path, false)
    }

    /**
      * Add a file to be downloaded with this Spark job on every node.
      * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
      * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
      * use `SparkFiles.get(fileName)` to find its download location.
      *
      * A directory can be given if the recursive option is set to true. Currently directories are only
      * supported for Hadoop-supported filesystems.
      */
    /*
    *将文件拉到所有worker节点，保证在每个节点都能访问到文件
    * 并且把数据保存到临时节点上
    *
    * 具体分为不同情况下载文件：local、hadoop、http、https、ftp
    *
    *************************************************
    * example：在Driver中获取分发出去的文件,        *
    * SparkFiles.get(path:String)获取添加的文件路径 *
    * var path = "/user/iteblog/ip.txt"             *
    * sc.addFile(path)                              *
    * val rdd = sc.textFile(SparkFiles.get(path))   *
    *
    * ***********************************************
    * example：在Exector获取到分发的文件            *
    * var path = "/user/iteblog/ip.txt"             *
    * sc.addFile(path)                              *
    * val rdd = sc.parallelize((0 to 10))           *
    * rdd.foreach{ index =>                         *
    *   val path = SparkFiles.get(path)             *
    * ......                                        *
    *}                                              *
    * ***********************************************
    * https://www.iteblog.com/archives/1704.html
    *
    * */
    def addFile(path: String, recursive: Boolean): Unit = {
        val uri = new URI(path)
        val schemeCorrectedPath = uri.getScheme match {
            case null | "local" => new File(path).getCanonicalFile.toURI.toString
            case _ => path
        }

        val hadoopPath = new Path(schemeCorrectedPath)
        val scheme = new URI(schemeCorrectedPath).getScheme
        if (!Array("http", "https", "ftp").contains(scheme)) {
            val fs = hadoopPath.getFileSystem(hadoopConfiguration)
            if (!fs.exists(hadoopPath)) {
                throw new FileNotFoundException(s"Added file $hadoopPath does not exist.")
            }
            val isDir = fs.isDirectory(hadoopPath)
            if (!isLocal && scheme == "file" && isDir) {
                throw new SparkException(s"addFile does not support local directories when not running " +
                        "local mode.")
            }
            if (!recursive && isDir) {
                throw new SparkException(s"Added file $hadoopPath is a directory and recursive is not " +
                        "turned on.")
            }
        }

        val key = if (!isLocal && scheme == "file") {
            env.httpFileServer.addFile(new File(uri.getPath))
        } else {
            schemeCorrectedPath
        }
        val timestamp = System.currentTimeMillis
        addedFiles(key) = timestamp

        // Fetch the file locally in case a job is executed using DAGScheduler.runLocally().
        Utils.fetchFile(path, new File(SparkFiles.getRootDirectory()), conf, env.securityManager,
            hadoopConfiguration, timestamp, useCache = false)

        logInfo("Added file " + path + " at " + key + " with timestamp " + addedFiles(key))
        postEnvironmentUpdate()
    }

    /**
      * :: DeveloperApi ::
      * Register a listener to receive up-calls from events that happen during execution.
      */
    @DeveloperApi
    def addSparkListener(listener: SparkListener) {
        listenerBus.addListener(listener)
    }

    /**
      * Express a preference to the cluster manager for a given total number of executors.
      * This can result in canceling pending requests or filing additional requests.
      * This is currently only supported in YARN mode. Return whether the request is received.
      */
    /*
    *
    * 动态分配资源相关
    * 跟schedulerBackend相关
    *
    * */
    private[spark] override def requestTotalExecutors(numExecutors: Int): Boolean = {
        assert(master.contains("yarn") || dynamicAllocationTesting,
            "Requesting executors is currently only supported in YARN mode")
        schedulerBackend match {
            case b: CoarseGrainedSchedulerBackend =>
                b.requestTotalExecutors(numExecutors)
            case _ =>
                logWarning("Requesting executors is only supported in coarse-grained mode")
                false
        }
    }

    /**
      * :: DeveloperApi ::
      * Request an additional number of executors from the cluster manager.
      * This is currently only supported in YARN mode. Return whether the request is received.
      */
    @DeveloperApi
    override def requestExecutors(numAdditionalExecutors: Int): Boolean = {
        assert(master.contains("yarn") || dynamicAllocationTesting,
            "Requesting executors is currently only supported in YARN mode")
        schedulerBackend match {
            case b: CoarseGrainedSchedulerBackend =>
                b.requestExecutors(numAdditionalExecutors)
            case _ =>
                logWarning("Requesting executors is only supported in coarse-grained mode")
                false
        }
    }

    /**
      * :: DeveloperApi ::
      * Request that the cluster manager kill the specified executors.
      * This is currently only supported in YARN mode. Return whether the request is received.
      */
    /*
    * 删除executor，只是删除了数据结构吗？？？？
    * */
    @DeveloperApi
    override def killExecutors(executorIds: Seq[String]): Boolean = {
        assert(master.contains("yarn") || dynamicAllocationTesting,
            "Killing executors is currently only supported in YARN mode")
        schedulerBackend match {
            case b: CoarseGrainedSchedulerBackend =>
                b.killExecutors(executorIds)
            case _ =>
                logWarning("Killing executors is only supported in coarse-grained mode")
                false
        }
    }

    /**
      * :: DeveloperApi ::
      * Request that cluster manager the kill the specified executor.
      * This is currently only supported in Yarn mode. Return whether the request is received.
      */
    @DeveloperApi
    override def killExecutor(executorId: String): Boolean = super.killExecutor(executorId)

    /** The version of Spark on which this application is running. */
    def version = SPARK_VERSION

    /**
      * Return a map from the slave to the max memory available for caching and the remaining
      * memory available for caching.
      */
    /*
    *
    *获取内存状态，都是从env.blockManager.master.getMemoryStatus.获取的
    * */
    def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
        assertNotStopped()
        env.blockManager.master.getMemoryStatus.map { case (blockManagerId, mem) =>
            (blockManagerId.host + ":" + blockManagerId.port, mem)
        }
    }

    /**
      * :: DeveloperApi ::
      * Return information about what RDDs are cached, if they are in mem or on disk, how much space
      * they take, etc.
      */
    @DeveloperApi
    def getRDDStorageInfo: Array[RDDInfo] = {
        assertNotStopped()
        val rddInfos = persistentRdds.values.map(RDDInfo.fromRdd).toArray
        StorageUtils.updateRddInfo(rddInfos, getExecutorStorageStatus)
        rddInfos.filter(_.isCached)
    }

    /**
      * Returns an immutable map of RDDs that have marked themselves as persistent via cache() call.
      * Note that this does not necessarily mean the caching or computation was successful.
      */

    /*
    *
    * 获取持久化的所有RDD的名字，不是内容，以下是例子
    * scala> val rdd2 = sc.makeRDD(10 to 1000)
    * # rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at makeRDD at <console>:27
    * scala> rdd2.cache.setName("rdd_2")
    * # res0: rdd2.type = rdd_2 ParallelCollectionRDD[1] at makeRDD at <console>:27
    * scala> sc.getPersistentRDDs
    * # res1: scala.collection.Map[Int,org.apache.spark.rdd.RDD[_]] = Map(1 -> rdd_2 ParallelCollectionRDD[1] at makeRDD at <console>:27)
    * scala> rdd1.cache.setName("foo")
    * # res2: rdd1.type = foo ParallelCollectionRDD[0] at makeRDD at <console>:27
    * scala> sc.getPersistentRDDs
    * # res3: scala.collection.Map[Int,org.apache.spark.rdd.RDD[_]] = Map(1 -> rdd_2 ParallelCollectionRDD[1] at makeRDD at <console>:27, 0 -> foo ParallelCollectionRDD[0] at makeRDD at <console>:27)
    *
    * */
    def getPersistentRDDs: Map[Int, RDD[_]] = persistentRdds.toMap

    /**
      * :: DeveloperApi ::
      * Return information about blocks stored in all of the slaves
      */
    /*
    *
    * 与存储相关的，就是BlockManager相关的，都是需要从SparkEnv调用获取的
    *
    * */
    @DeveloperApi
    def getExecutorStorageStatus: Array[StorageStatus] = {
        assertNotStopped()
        env.blockManager.master.getStorageStatus
    }

    /**
      * :: DeveloperApi ::
      * Return pools for fair scheduler
      */
    @DeveloperApi
    def getAllPools: Seq[Schedulable] = {
        assertNotStopped()
        // TODO(xiajunluan): We should take nested pools into account
        taskScheduler.rootPool.schedulableQueue.toSeq
    }

    /**
      * :: DeveloperApi ::
      * Return the pool associated with the given name, if one exists
      */
    @DeveloperApi
    def getPoolForName(pool: String): Option[Schedulable] = {
        assertNotStopped()
        Option(taskScheduler.rootPool.schedulableNameToSchedulable.get(pool))
    }

    /**
      * Return current scheduling mode
      */
    /*
    *
    * 返回TaskScheduler的调度模式：FAIR, FIFO, NONE
    * https://blog.csdn.net/dabokele/article/details/51526048
    * */
    def getSchedulingMode: SchedulingMode.SchedulingMode = {
        assertNotStopped()
        taskScheduler.schedulingMode
    }

    /**
      * Clear the job's list of files added by `addFile` so that they do not get downloaded to
      * any new nodes.
      *
      * 清理addFile下载的文件列表，在新的节点上不再下载
      */
    @deprecated("adding files no longer creates local copies that need to be deleted", "1.0.0")
    def clearFiles() {
        addedFiles.clear()
    }

    /**
      * Gets the locality information associated with the partition in a particular rdd
      *
      * @param rdd       of interest
      * @param partition to be looked up for locality
      * @return list of preferred locations for the partition
      */
    /*
    *
    * 数据本地化，返回该partition对应的task的优先位置列表，调用链如下：
    * sparkContext.getPreferredLocs->
    * dagScheduler.getPreferredLocs->
    * getPreferredLocsInternal->
    * rdd.preferredLocations
    * */
    private[spark] def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
        dagScheduler.getPreferredLocs(rdd, partition)
    }

    /**
      * Register an RDD to be persisted in memory and/or disk storage
      */
    /*
    *
    * 这就是所谓的持久化，调用sc，将（rdd.id,rdd）存在了内存中
    * private[spark] val persistentRdds = new TimeStampedWeakValueHashMap[Int, RDD[_]]
    *
    * */
    private[spark] def persistRDD(rdd: RDD[_]) {
        persistentRdds(rdd.id) = rdd
    }

    /**
      * Unpersist an RDD from memory and/or disk storage
      */
    /*
    *
    * 取消持久化的RDD，需要从BlockManager中removeRdd，因为存储管理，都是在BlockManager中进行的
    * */
    private[spark] def unpersistRDD(rddId: Int, blocking: Boolean = true) {
        env.blockManager.master.removeRdd(rddId, blocking)
        persistentRdds.remove(rddId)
        listenerBus.post(SparkListenerUnpersistRDD(rddId))
    }

    /**
      * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
      * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
      * filesystems), an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
      */
    /*
    *
    * 下载依赖的jar，这是具体是实现过程，根据path格式区别下载
    *
    * */
    def addJar(path: String) {
        if (path == null) {
            logWarning("null specified as parameter to addJar")
        } else {
            var key = ""
            if (path.contains("\\")) {
                // For local paths with backslashes on Windows, URI throws an exception
                key = env.httpFileServer.addJar(new File(path))
            } else {
                val uri = new URI(path)
                key = uri.getScheme match {
                    // A JAR file which exists only on the driver node
                    case null | "file" =>
                        // yarn-standalone is deprecated, but still supported
                        if (SparkHadoopUtil.get.isYarnMode() &&
                                (master == "yarn-standalone" || master == "yarn-cluster")) {
                            // In order for this to work in yarn-cluster mode the user must specify the
                            // --addJars option to the client to upload the file into the distributed cache
                            // of the AM to make it show up in the current working directory.
                            val fileName = new Path(uri.getPath).getName()
                            try {
                                env.httpFileServer.addJar(new File(fileName))
                            } catch {
                                case e: Exception =>
                                    // For now just log an error but allow to go through so spark examples work.
                                    // The spark examples don't really need the jar distributed since its also
                                    // the app jar.
                                    logError("Error adding jar (" + e + "), was the --addJars option used?")
                                    null
                            }
                        } else {
                            try {
                                env.httpFileServer.addJar(new File(uri.getPath))
                            } catch {
                                case exc: FileNotFoundException =>
                                    logError(s"Jar not found at $path")
                                    null
                                case e: Exception =>
                                    // For now just log an error but allow to go through so spark examples work.
                                    // The spark examples don't really need the jar distributed since its also
                                    // the app jar.
                                    logError("Error adding jar (" + e + "), was the --addJars option used?")
                                    null
                            }
                        }
                    // A JAR file which exists locally on every worker node
                    case "local" =>
                        "file:" + uri.getPath
                    case _ =>
                        path
                }
            }
            if (key != null) {
                addedJars(key) = System.currentTimeMillis
                logInfo("Added JAR " + path + " at " + key + " with timestamp " + addedJars(key))
            }
        }
        postEnvironmentUpdate()
    }

    /**
      * Clear the job's list of JARs added by `addJar` so that they do not get downloaded to
      * any new nodes.
      */
    @deprecated("adding jars no longer creates local copies that need to be deleted", "1.0.0")
    def clearJars() {
        addedJars.clear()
    }

    /** Shut down the SparkContext. */
    def stop() {
        SparkContext.SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            if (!stopped) {
                /*
                *
                * 以下是需要关闭的所有模块：ui、SparkEnv、DAGScheduler、TaskScheduler、listenerBus、metadataCleaner
                *
                * */
                stopped = true
                postApplicationEnd()
                ui.foreach(_.stop())
                env.metricsSystem.report()
                metadataCleaner.cancel()
                cleaner.foreach(_.stop())
                executorAllocationManager.foreach(_.stop())
                dagScheduler.stop()
                dagScheduler = null
                listenerBus.stop()
                eventLogger.foreach(_.stop())
                env.actorSystem.stop(heartbeatReceiver)
                progressBar.foreach(_.stop())
                taskScheduler = null
                // TODO: Cache.stop()?
                env.stop()
                SparkEnv.set(null)
                logInfo("Successfully stopped SparkContext")
                SparkContext.clearActiveContext()
            } else {
                logInfo("SparkContext already stopped")
            }
        }
    }


    /**
      * Get Spark's home location from either a value set through the constructor,
      * or the spark.home Java property, or the SPARK_HOME environment variable
      * (in that order of preference). If neither of these is set, return None.
      */
    private[spark] def getSparkHome(): Option[String] = {
        conf.getOption("spark.home").orElse(Option(System.getenv("SPARK_HOME")))
    }

    /** ***********************************以下几个CallSite相关函数暂时不清楚作用？ ****************************************/
    /**
      * Set the thread-local property for overriding the call sites
      * of actions and RDDs.
      */
    def setCallSite(shortCallSite: String) {
        setLocalProperty(CallSite.SHORT_FORM, shortCallSite)
    }

    /**
      * Set the thread-local property for overriding the call sites
      * of actions and RDDs.
      */
    private[spark] def setCallSite(callSite: CallSite) {
        setLocalProperty(CallSite.SHORT_FORM, callSite.shortForm)
        setLocalProperty(CallSite.LONG_FORM, callSite.longForm)
    }

    /**
      * Clear the thread-local property for overriding the call sites
      * of actions and RDDs.
      */
    def clearCallSite() {
        setLocalProperty(CallSite.SHORT_FORM, null)
        setLocalProperty(CallSite.LONG_FORM, null)
    }

    /**
      * Capture the current user callsite and return a formatted version for printing. If the user
      * has overridden the call site using `setCallSite()`, this will return the user's version.
      */
    /*
    * https://blog.csdn.net/sinat_35045195/article/details/79676701
    * */
    private[spark] def getCallSite(): CallSite = {
        Option(getLocalProperty(CallSite.SHORT_FORM)).map { case shortCallSite =>
            val longCallSite = Option(getLocalProperty(CallSite.LONG_FORM)).getOrElse("")
            CallSite(shortCallSite, longCallSite)
        }.getOrElse(Utils.getCallSite())
    }

    /** *****************************************以下几个runJob函数是层层调用的关系，第一个是最底层的runJob **********************************************/
    /**
      * Run a function on a given set of partitions in an RDD and pass the results to the given
      * 在一个RDD的所有分区上运行一个function
      * handler function. This is the main entry point for all actions in Spark. The allowLocal
      * flag specifies whether the scheduler can run the computation on the driver rather than
      * shipping it out to the cluster, for short actions like first().
      */
    /*
    *
    * action()算子执行时，首先调用SparkContext.runJob
    * 研究一下ClassTag、TaskContext这两个类
    *
    *SparkContext.runJob->DAGScheduler.runJob
    *
    * */
    def runJob[T, U: ClassTag](
                                      rdd: RDD[T],
                                      func: (TaskContext, Iterator[T]) => U,
                                      partitions: Seq[Int],
                                      allowLocal: Boolean,
                                      resultHandler: (Int, U) => Unit) {
        if (stopped) {
            throw new IllegalStateException("SparkContext has been shutdown")
        }
        val callSite = getCallSite
        val cleanedFunc = clean(func)
        logInfo("Starting job: " + callSite.shortForm)
        if (conf.getBoolean("spark.logLineage", false)) {
            logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
        }
        dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, allowLocal,
            resultHandler, localProperties.get)
        progressBar.foreach(_.finishAll())
        /*
        *
        * 为什么要持久化？？？
        * */
        rdd.doCheckpoint()
    }

    /**
      * Run a function on a given set of partitions in an RDD and return the results as an array. The
      * allowLocal flag specifies whether the scheduler can run the computation on the driver rather
      * than shipping it out to the cluster, for short actions like first().
      */
    def runJob[T, U: ClassTag](
                                      rdd: RDD[T],
                                      func: (TaskContext, Iterator[T]) => U,
                                      partitions: Seq[Int],
                                      allowLocal: Boolean
                              ): Array[U] = {
        val results = new Array[U](partitions.size)
        runJob[T, U](rdd, func, partitions, allowLocal, (index, res) => results(index) = res)
        results
    }

    /**
      * Run a job on a given set of partitions of an RDD, but take a function of type
      * `Iterator[T] => U` instead of `(TaskContext, Iterator[T]) => U`.
      */
    def runJob[T, U: ClassTag](
                                      rdd: RDD[T],
                                      func: Iterator[T] => U,
                                      partitions: Seq[Int],
                                      allowLocal: Boolean
                              ): Array[U] = {
        runJob(rdd, (context: TaskContext, iter: Iterator[T]) => func(iter), partitions, allowLocal)
    }

    /**
      * Run a job on all partitions in an RDD and return the results in an array.
      */
    def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
        runJob(rdd, func, 0 until rdd.partitions.size, false)
    }

    /**
      * Run a job on all partitions in an RDD and return the results in an array.
      */
    def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
        runJob(rdd, func, 0 until rdd.partitions.size, false)
    }

    /**
      * Run a job on all partitions in an RDD and pass the results to a handler function.
      */
    def runJob[T, U: ClassTag](
                                      rdd: RDD[T],
                                      processPartition: (TaskContext, Iterator[T]) => U,
                                      resultHandler: (Int, U) => Unit) {
        runJob[T, U](rdd, processPartition, 0 until rdd.partitions.size, false, resultHandler)
    }

    /**
      * Run a job on all partitions in an RDD and pass the results to a handler function.
      */
    def runJob[T, U: ClassTag](
                                      rdd: RDD[T],
                                      processPartition: Iterator[T] => U,
                                      resultHandler: (Int, U) => Unit) {
        val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
        runJob[T, U](rdd, processFunc, 0 until rdd.partitions.size, false, resultHandler)
    }

    /**
      * :: DeveloperApi ::
      * Run a job that can return approximate results.
      */
    /*
    *
    * 不明白该函数的意思
    *
    *
    * */
    @DeveloperApi
    def runApproximateJob[T, U, R](
                                          rdd: RDD[T],
                                          func: (TaskContext, Iterator[T]) => U,
                                          evaluator: ApproximateEvaluator[U, R],
                                          timeout: Long): PartialResult[R] = {
        assertNotStopped()
        val callSite = getCallSite
        logInfo("Starting job: " + callSite.shortForm)
        val start = System.nanoTime
        val result = dagScheduler.runApproximateJob(rdd, func, evaluator, callSite, timeout,
            localProperties.get)
        logInfo(
            "Job finished: " + callSite.shortForm + ", took " + (System.nanoTime - start) / 1e9 + " s")
        result
    }

    /**
      * :: Experimental ::
      * Submit a job for execution and return a FutureJob holding the result.
      */
    @Experimental
    def submitJob[T, U, R](
                                  rdd: RDD[T],
                                  processPartition: Iterator[T] => U,
                                  partitions: Seq[Int],
                                  resultHandler: (Int, U) => Unit,
                                  resultFunc: => R): SimpleFutureAction[R] = {
        assertNotStopped()
        val cleanF = clean(processPartition)
        val callSite = getCallSite
        val waiter = dagScheduler.submitJob(
            rdd,
            (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
            partitions,
            callSite,
            allowLocal = false,
            resultHandler,
            localProperties.get)
        new SimpleFutureAction(waiter, resultFunc)
    }

    /**
      * Cancel active jobs for the specified group. See [[org.apache.spark.SparkContext.setJobGroup]]
      * for more information.
      */
    /*
    *
    * 清除某个组的活跃的Job，意味着：这些Job可以通过代码中的SparkContext对象，调用该函数 手动关闭Job
    *
    * */
    def cancelJobGroup(groupId: String) {
        assertNotStopped()
        dagScheduler.cancelJobGroup(groupId)
    }

    /** Cancel all jobs that have been scheduled or are running.  */
    def cancelAllJobs() {
        assertNotStopped()
        dagScheduler.cancelAllJobs()
    }

    /** Cancel a given job if it's scheduled or running */
    private[spark] def cancelJob(jobId: Int) {
        dagScheduler.cancelJob(jobId)
    }

    /** Cancel a given stage and all jobs associated with it */
    private[spark] def cancelStage(stageId: Int) {
        dagScheduler.cancelStage(stageId)
    }

    /**
      * Clean a closure to make it ready to serialized and send to tasks
      * (removes unreferenced variables in $outer's, updates REPL variables)
      * If <tt>checkSerializable</tt> is set, <tt>clean</tt> will also proactively
      * check to see if <tt>f</tt> is serializable and throw a <tt>SparkException</tt>
      * if not.
      *
      * @param f                 the closure to clean
      * @param checkSerializable whether or not to immediately check <tt>f</tt> for serializability
      *                          //@throws  <tt>SparkException<tt> if <tt>checkSerializable</tt> is set but <tt>f</tt> is not
      *                          serializable
      */
    private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
        ClosureCleaner.clean(f, checkSerializable)
        f
    }

    /**
      * Set the directory under which RDDs are going to be checkpointed. The directory must
      * be a HDFS path if running on a cluster.
      */
    /*
    *
    * 设置checkpoint目录，如果是集群，写HDFS目录
    * */
    def setCheckpointDir(directory: String) {
        checkpointDir = Option(directory).map { dir =>
            /*
            * 写到HDFS里
            * */
            val path = new Path(dir, UUID.randomUUID().toString)
            val fs = path.getFileSystem(hadoopConfiguration)
            fs.mkdirs(path)
            fs.getFileStatus(path).getPath.toString
        }
    }

    def getCheckpointDir = checkpointDir

    /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD). */
    /*
    *
    * TaskScheduler中调用defaultParallelism，不同的taskScheduler会调用不同的backend实现不同的defaultParallelism
    * taskScheduler.defaultParallelism()->
    * TaskSchedulerImpl.defaultParallelism()->
    * backend.defaultParallelism(),
    *
    * conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
    * */
    def defaultParallelism: Int = {
        assertNotStopped()
        taskScheduler.defaultParallelism
    }

    /** Default min number of partitions for Hadoop RDDs when not given by user */
    /*
    *
    * 跟2比较
    * */
    @deprecated("use defaultMinPartitions", "1.0.0")
    def defaultMinSplits: Int = math.min(defaultParallelism, 2)

    /**
      * Default min number of partitions for Hadoop RDDs when not given by user
      * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
      * The reasons for this are discussed in https://github.com/mesos/spark/pull/718
      */
    def defaultMinPartitions: Int = math.min(defaultParallelism, 2)

    private val nextShuffleId = new AtomicInteger(0)

    private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()

    private val nextRddId = new AtomicInteger(0)

    /** Register a new RDD, returning its RDD ID */
    private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

    /**
      * Registers listeners specified in spark.extraListeners, then starts the listener bus.
      * This should be called after all internal listeners have been registered with the listener bus
      * (e.g. after the web UI and event logging listeners have been registered).
      */
    /*
    *
    * 所有监听器都注册完后，启动总线 start()
    * (e.g. after the web UI and event logging listeners have been registered).
    *
    * */
    private def setupAndStartListenerBus(): Unit = {
        // Use reflection to instantiate listeners specified via `spark.extraListeners`
        try {
            val listenerClassNames: Seq[String] =
                conf.get("spark.extraListeners", "").split(',').map(_.trim).filter(_ != "")
            for (className <- listenerClassNames) {
                // Use reflection to find the right constructor
                val constructors = {
                    val listenerClass = Class.forName(className)
                    listenerClass.getConstructors.asInstanceOf[Array[Constructor[_ <: SparkListener]]]
                }
                val constructorTakingSparkConf = constructors.find { c =>
                    c.getParameterTypes.sameElements(Array(classOf[SparkConf]))
                }
                lazy val zeroArgumentConstructor = constructors.find { c =>
                    c.getParameterTypes.isEmpty
                }
                val listener: SparkListener = {
                    if (constructorTakingSparkConf.isDefined) {
                        constructorTakingSparkConf.get.newInstance(conf)
                    } else if (zeroArgumentConstructor.isDefined) {
                        zeroArgumentConstructor.get.newInstance()
                    } else {
                        throw new SparkException(
                            s"$className did not have a zero-argument constructor or a" +
                                    " single-argument constructor that accepts SparkConf. Note: if the class is" +
                                    " defined inside of another Scala class, then its constructors may accept an" +
                                    " implicit parameter that references the enclosing class; in this case, you must" +
                                    " define the listener as a top-level class in order to prevent this extra" +
                                    " parameter from breaking Spark's ability to find a valid constructor.")
                    }
                }
                listenerBus.addListener(listener)
                logInfo(s"Registered listener $className")
            }
        } catch {
            case e: Exception =>
                try {
                    stop()
                } finally {
                    throw new SparkException(s"Exception when registering SparkListener", e)
                }
        }

        listenerBus.start()
    }

    /*
    *
    * ***********************************************下面是给总线发送消息的*****************************************************
    *
    * */
    /** Post the application start event */
    private def postApplicationStart() {
        // Note: this code assumes that the task scheduler has been initialized and has contacted
        // the cluster manager to get an application ID (in case the cluster manager provides one).
        listenerBus.post(SparkListenerApplicationStart(appName, Some(applicationId),
            startTime, sparkUser))
    }

    /** Post the application end event */
    private def postApplicationEnd() {
        listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis))
    }

    /** Post the environment update event once the task scheduler is ready */
    private def postEnvironmentUpdate() {
        if (taskScheduler != null) {
            val schedulingMode = getSchedulingMode.toString
            val addedJarPaths = addedJars.keys.toSeq
            val addedFilePaths = addedFiles.keys.toSeq
            val environmentDetails = SparkEnv.environmentDetails(conf, schedulingMode, addedJarPaths,
                addedFilePaths)
            val environmentUpdate = SparkListenerEnvironmentUpdate(environmentDetails)
            listenerBus.post(environmentUpdate)
        }
    }

    /** Called by MetadataCleaner to clean up the persistentRdds map periodically */
    private[spark] def cleanup(cleanupTime: Long) {
        persistentRdds.clearOldValues(cleanupTime)
    }

    // In order to prevent multiple SparkContexts from being active at the same time, mark this
    // context as having finished construction.
    // NOTE: this must be placed at the end of the SparkContext constructor.
    SparkContext.setActiveContext(this, allowMultipleContexts)
}

/**
  * The SparkContext object contains a number of implicit conversions and parameters for use with
  * various Spark features.
  */
object SparkContext extends Logging {

    /**
      * Lock that guards access to global variables that track SparkContext construction.
      */
    private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()

    /**
      * The active, fully-constructed SparkContext.  If no SparkContext is active, then this is `None`.
      *
      * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK
      */
    private var activeContext: Option[SparkContext] = None

    /**
      * Points to a partially-constructed SparkContext if some thread is in the SparkContext
      * constructor, or `None` if no SparkContext is being constructed.
      *
      * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK
      */
    private var contextBeingConstructed: Option[SparkContext] = None

    /**
      * Called to ensure that no other SparkContext is running in this JVM.
      *
      * Throws an exception if a running context is detected and logs a warning if another thread is
      * constructing a SparkContext.  This warning is necessary because the current locking scheme
      * prevents us from reliably distinguishing between cases where another context is being
      * constructed and cases where another constructor threw an exception.
      */
    /*
    *
    * 检查没有其他SparkContext运行
    * */
    private def assertNoOtherContextIsRunning(
                                                     sc: SparkContext,
                                                     allowMultipleContexts: Boolean): Unit = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            contextBeingConstructed.foreach { otherContext =>
                if (otherContext ne sc) { // checks for reference equality
                    // Since otherContext might point to a partially-constructed context, guard against
                    // its creationSite field being null:
                    val otherContextCreationSite =
                    Option(otherContext.creationSite).map(_.longForm).getOrElse("unknown location")
                    val warnMsg = "Another SparkContext is being constructed (or threw an exception in its" +
                            " constructor).  This may indicate an error, since only one SparkContext may be" +
                            " running in this JVM (see SPARK-2243)." +
                            s" The other SparkContext was created at:\n$otherContextCreationSite"
                    logWarning(warnMsg)
                }

                activeContext.foreach { ctx =>
                    val errMsg = "Only one SparkContext may be running in this JVM (see SPARK-2243)." +
                            " To ignore this error, set spark.driver.allowMultipleContexts = true. " +
                            s"The currently running SparkContext was created at:\n${ctx.creationSite.longForm}"
                    val exception = new SparkException(errMsg)
                    if (allowMultipleContexts) {
                        logWarning("Multiple running SparkContexts detected in the same JVM!", exception)
                    } else {
                        throw exception
                    }
                }
            }
        }
    }

    /**
      * Called at the beginning of the SparkContext constructor to ensure that no SparkContext is
      * running.  Throws an exception if a running context is detected and logs a warning if another
      * thread is constructing a SparkContext.  This warning is necessary because the current locking
      * scheme prevents us from reliably distinguishing between cases where another context is being
      * constructed and cases where another constructor threw an exception.
      */
    private[spark] def markPartiallyConstructed(
                                                       sc: SparkContext,
                                                       allowMultipleContexts: Boolean): Unit = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            assertNoOtherContextIsRunning(sc, allowMultipleContexts)
            contextBeingConstructed = Some(sc)
        }
    }

    /**
      * Called at the end of the SparkContext constructor to ensure that no other SparkContext has
      * raced with this constructor and started.
      */
    private[spark] def setActiveContext(
                                               sc: SparkContext,
                                               allowMultipleContexts: Boolean): Unit = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            assertNoOtherContextIsRunning(sc, allowMultipleContexts)
            contextBeingConstructed = None
            activeContext = Some(sc)
        }
    }

    /**
      * Clears the active SparkContext metadata.  This is called by `SparkContext#stop()`.  It's
      * also called in unit tests to prevent a flood of warnings from test suites that don't / can't
      * properly clean up their SparkContexts.
      */
    private[spark] def clearActiveContext(): Unit = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            activeContext = None
        }
    }

    private[spark] val SPARK_JOB_DESCRIPTION = "spark.job.description"

    private[spark] val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"

    private[spark] val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"

    private[spark] val DRIVER_IDENTIFIER = "<driver>"

    // The following deprecated objects have already been copied to `object AccumulatorParam` to
    // make the compiler find them automatically. They are duplicate codes only for backward
    // compatibility, please update `object AccumulatorParam` accordingly if you plan to modify the
    // following ones.

    @deprecated("Replaced by implicit objects in AccumulatorParam. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    object DoubleAccumulatorParam extends AccumulatorParam[Double] {
        def addInPlace(t1: Double, t2: Double): Double = t1 + t2

        def zero(initialValue: Double) = 0.0
    }

    @deprecated("Replaced by implicit objects in AccumulatorParam. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    object IntAccumulatorParam extends AccumulatorParam[Int] {
        def addInPlace(t1: Int, t2: Int): Int = t1 + t2

        def zero(initialValue: Int) = 0
    }

    @deprecated("Replaced by implicit objects in AccumulatorParam. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    object LongAccumulatorParam extends AccumulatorParam[Long] {
        def addInPlace(t1: Long, t2: Long) = t1 + t2

        def zero(initialValue: Long) = 0L
    }

    @deprecated("Replaced by implicit objects in AccumulatorParam. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    object FloatAccumulatorParam extends AccumulatorParam[Float] {
        def addInPlace(t1: Float, t2: Float) = t1 + t2

        def zero(initialValue: Float) = 0f
    }

    // The following deprecated functions have already been moved to `object RDD` to
    // make the compiler find them automatically. They are still kept here for backward compatibility
    // and just call the corresponding functions in `object RDD`.

    @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
                                   (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) = {
        RDD.rddToPairRDDFunctions(rdd)
    }

    @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]) = RDD.rddToAsyncRDDActions(rdd)

    @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    def rddToSequenceFileRDDFunctions[K <% Writable : ClassTag, V <% Writable : ClassTag](
                                                                                                 rdd: RDD[(K, V)]) = {
        val kf = implicitly[K => Writable]
        val vf = implicitly[V => Writable]
        // Set the Writable class to null and `SequenceFileRDDFunctions` will use Reflection to get it
        implicit val keyWritableFactory = new WritableFactory[K](_ => null, kf)
        implicit val valueWritableFactory = new WritableFactory[V](_ => null, vf)
        RDD.rddToSequenceFileRDDFunctions(rdd)
    }

    @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    def rddToOrderedRDDFunctions[K: Ordering : ClassTag, V: ClassTag](
                                                                             rdd: RDD[(K, V)]) =
        RDD.rddToOrderedRDDFunctions(rdd)

    @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]) = RDD.doubleRDDToDoubleRDDFunctions(rdd)

    @deprecated("Replaced by implicit functions in the RDD companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T]) =
        RDD.numericRDDToDoubleRDDFunctions(rdd)

    // The following deprecated functions have already been moved to `object WritableFactory` to
    // make the compiler find them automatically. They are still kept here for backward compatibility.

    @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    implicit def intToIntWritable(i: Int): IntWritable = new IntWritable(i)

    @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    implicit def longToLongWritable(l: Long): LongWritable = new LongWritable(l)

    @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    implicit def floatToFloatWritable(f: Float): FloatWritable = new FloatWritable(f)

    @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    implicit def doubleToDoubleWritable(d: Double): DoubleWritable = new DoubleWritable(d)

    @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    implicit def boolToBoolWritable(b: Boolean): BooleanWritable = new BooleanWritable(b)

    @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    implicit def bytesToBytesWritable(aob: Array[Byte]): BytesWritable = new BytesWritable(aob)

    @deprecated("Replaced by implicit functions in the WritableFactory companion object. This is " +
            "kept here only for backward compatibility.", "1.3.0")
    implicit def stringToText(s: String): Text = new Text(s)

    private implicit def arrayToArrayWritable[T <% Writable : ClassTag](arr: Traversable[T])
    : ArrayWritable = {
        def anyToWritable[U <% Writable](u: U): Writable = u

        new ArrayWritable(classTag[T].runtimeClass.asInstanceOf[Class[Writable]],
            arr.map(x => anyToWritable(x)).toArray)
    }

    // The following deprecated functions have already been moved to `object WritableConverter` to
    // make the compiler find them automatically. They are still kept here for backward compatibility
    // and just call the corresponding functions in `object WritableConverter`.

    @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    def intWritableConverter(): WritableConverter[Int] =
        WritableConverter.intWritableConverter()

    @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    def longWritableConverter(): WritableConverter[Long] =
        WritableConverter.longWritableConverter()

    @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    def doubleWritableConverter(): WritableConverter[Double] =
        WritableConverter.doubleWritableConverter()

    @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    def floatWritableConverter(): WritableConverter[Float] =
        WritableConverter.floatWritableConverter()

    @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    def booleanWritableConverter(): WritableConverter[Boolean] =
        WritableConverter.booleanWritableConverter()

    @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    def bytesWritableConverter(): WritableConverter[Array[Byte]] =
        WritableConverter.bytesWritableConverter()

    @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    def stringWritableConverter(): WritableConverter[String] =
        WritableConverter.stringWritableConverter()

    @deprecated("Replaced by implicit functions in WritableConverter. This is kept here only for " +
            "backward compatibility.", "1.3.0")
    def writableWritableConverter[T <: Writable](): WritableConverter[T] =
        WritableConverter.writableWritableConverter()

    /**
      * Find the JAR from which a given class was loaded, to make it easy for users to pass
      * their JARs to SparkContext.
      */
    def jarOfClass(cls: Class[_]): Option[String] = {
        val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
        if (uri != null) {
            val uriStr = uri.toString
            if (uriStr.startsWith("jar:file:")) {
                // URI will be of the form "jar:file:/path/foo.jar!/package/cls.class",
                // so pull out the /path/foo.jar
                Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
            } else {
                None
            }
        } else {
            None
        }
    }

    /**
      * Find the JAR that contains the class of a particular object, to make it easy for users
      * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
      * your driver program.
      */
    def jarOfObject(obj: AnyRef): Option[String] = jarOfClass(obj.getClass)

    /**
      * Creates a modified version of a SparkConf with the parameters that can be passed separately
      * to SparkContext, to make it easier to write SparkContext's constructors. This ignores
      * parameters that are passed as the default value of null, instead of throwing an exception
      * like SparkConf would.
      */
    private[spark] def updatedConf(
                                          conf: SparkConf,
                                          master: String,
                                          appName: String,
                                          sparkHome: String = null,
                                          jars: Seq[String] = Nil,
                                          environment: Map[String, String] = Map()): SparkConf = {
        val res = conf.clone()
        res.setMaster(master)
        res.setAppName(appName)
        if (sparkHome != null) {
            res.setSparkHome(sparkHome)
        }
        if (jars != null && !jars.isEmpty) {
            res.setJars(jars)
        }
        res.setExecutorEnv(environment.toSeq)
        res
    }

    /**
      * Create a task scheduler based on a given master URL.
      * Return a 2-tuple of the scheduler backend and the task scheduler.
      */
    /*
    *
    * 返回的是两个：SchedulerBackend, TaskScheduler
    * 为什么没有yarn模式？？？
    *
    * */
    private def createTaskScheduler(
                                           sc: SparkContext,
                                           master: String): (SchedulerBackend, TaskScheduler) = {
        // Regular expression used for local[N] and local[*] master formats
        val LOCAL_N_REGEX =
            """local\[([0-9]+|\*)\]""".r
        // Regular expression for local[N, maxRetries], used in tests with failing tasks
        val LOCAL_N_FAILURES_REGEX =
            """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
        // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
        val LOCAL_CLUSTER_REGEX =
            """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
        // Regular expression for connecting to Spark deploy clusters
        val SPARK_REGEX =
            """spark://(.*)""".r
        // Regular expression for connection to Mesos cluster by mesos:// or zk:// url
        val MESOS_REGEX =
            """(mesos|zk)://.*""".r
        // Regular expression for connection to Simr cluster
        val SIMR_REGEX =
            """simr://(.*)""".r

        // When running locally, don't try to re-execute tasks on failure.
        val MAX_LOCAL_TASK_FAILURES = 1

        /*
        * 根据master的url解析不同的启动模式
        * TaskSchedulerImpl：local、local_N、spark、spark-cluster
        * backend：LocalBackend、SparkDeploySchedulerBackend
        *
        * yarn-cluster：org.apache.spark.scheduler.cluster.YarnClusterScheduler；org.apache.spark.scheduler.cluster.YarnClusterSchedulerBackend
        * yarn-client：org.apache.spark.scheduler.cluster.YarnScheduler；org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend
        * */
        /*
        *
        * 1、创建TaskScheduler
        * 2、创建backend
        * 3、初始化backend，构建schedulableBuilder（FIFO、FAIR）
        *
        * */
        master match {
            case "local" =>
                val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
                val backend = new LocalBackend(scheduler, 1)
                /*
                * TaskSchedulerImpl初始化backend，调用了schedulableBuilder构建调度器
                *
                * TaskSchedulerImpl.initialize(backend)->schedulableBuilder(rootPool)
                * TaskScheduler.initialize（backend）是把backend赋与了TaskScheduler中的backend变量。
                * */
                scheduler.initialize(backend)
                (backend, scheduler)

            case LOCAL_N_REGEX(threads) =>
                def localCpuCount = Runtime.getRuntime.availableProcessors()

                // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
                val threadCount = if (threads == "*") localCpuCount else threads.toInt
                if (threadCount <= 0) {
                    throw new SparkException(s"Asked to run locally with $threadCount threads")
                }
                val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
                val backend = new LocalBackend(scheduler, threadCount)
                scheduler.initialize(backend)
                (backend, scheduler)

            case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
                def localCpuCount = Runtime.getRuntime.availableProcessors()

                // local[*, M] means the number of cores on the computer with M failures
                // local[N, M] means exactly N threads with M failures
                val threadCount = if (threads == "*") localCpuCount else threads.toInt
                val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
                val backend = new LocalBackend(scheduler, threadCount)
                scheduler.initialize(backend)
                (backend, scheduler)

            case SPARK_REGEX(sparkUrl) =>
                val scheduler = new TaskSchedulerImpl(sc)
                val masterUrls = sparkUrl.split(",").map("spark://" + _)
                /*
                *
                * SparkDeploySchedulerBackend继承自CoarseGrainedSchedulerBackend
                * */
                val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls)
                scheduler.initialize(backend)
                (backend, scheduler)

            case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
                // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
                val memoryPerSlaveInt = memoryPerSlave.toInt
                if (sc.executorMemory > memoryPerSlaveInt) {
                    throw new SparkException(
                        "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
                            memoryPerSlaveInt, sc.executorMemory))
                }

                val scheduler = new TaskSchedulerImpl(sc)
                val localCluster = new LocalSparkCluster(
                    numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt, sc.conf)
                val masterUrls = localCluster.start()
                val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls)
                scheduler.initialize(backend)
                backend.shutdownCallback = (backend: SparkDeploySchedulerBackend) => {
                    localCluster.stop()
                }
                (backend, scheduler)

            case "yarn-standalone" | "yarn-cluster" =>
                if (master == "yarn-standalone") {
                    logWarning(
                        "\"yarn-standalone\" is deprecated as of Spark 1.0. Use \"yarn-cluster\" instead.")
                }
                val scheduler = try {
                    val clazz = Class.forName("org.apache.spark.scheduler.cluster.YarnClusterScheduler")
                    val cons = clazz.getConstructor(classOf[SparkContext])
                    cons.newInstance(sc).asInstanceOf[TaskSchedulerImpl]
                } catch {
                    // TODO: Enumerate the exact reasons why it can fail
                    // But irrespective of it, it means we cannot proceed !
                    case e: Exception => {
                        throw new SparkException("YARN mode not available ?", e)
                    }
                }
                val backend = try {
                    val clazz =
                        Class.forName("org.apache.spark.scheduler.cluster.YarnClusterSchedulerBackend")
                    val cons = clazz.getConstructor(classOf[TaskSchedulerImpl], classOf[SparkContext])
                    cons.newInstance(scheduler, sc).asInstanceOf[CoarseGrainedSchedulerBackend]
                } catch {
                    case e: Exception => {
                        throw new SparkException("YARN mode not available ?", e)
                    }
                }
                scheduler.initialize(backend)
                (backend, scheduler)

            case "yarn-client" =>
                val scheduler = try {
                    val clazz =
                        Class.forName("org.apache.spark.scheduler.cluster.YarnScheduler")
                    val cons = clazz.getConstructor(classOf[SparkContext])
                    cons.newInstance(sc).asInstanceOf[TaskSchedulerImpl]

                } catch {
                    case e: Exception => {
                        throw new SparkException("YARN mode not available ?", e)
                    }
                }

                val backend = try {
                    val clazz =
                        Class.forName("org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend")
                    val cons = clazz.getConstructor(classOf[TaskSchedulerImpl], classOf[SparkContext])
                    cons.newInstance(scheduler, sc).asInstanceOf[CoarseGrainedSchedulerBackend]
                } catch {
                    case e: Exception => {
                        throw new SparkException("YARN mode not available ?", e)
                    }
                }

                scheduler.initialize(backend)
                (backend, scheduler)

            case mesosUrl@MESOS_REGEX(_) =>
                MesosNativeLibrary.load()
                val scheduler = new TaskSchedulerImpl(sc)
                val coarseGrained = sc.conf.getBoolean("spark.mesos.coarse", false)
                val url = mesosUrl.stripPrefix("mesos://") // strip scheme from raw Mesos URLs
            val backend = if (coarseGrained) {
                new CoarseMesosSchedulerBackend(scheduler, sc, url)
            } else {
                new MesosSchedulerBackend(scheduler, sc, url)
            }
                scheduler.initialize(backend)
                (backend, scheduler)

            case SIMR_REGEX(simrUrl) =>
                val scheduler = new TaskSchedulerImpl(sc)
                val backend = new SimrSchedulerBackend(scheduler, sc, simrUrl)
                scheduler.initialize(backend)
                (backend, scheduler)

            case _ =>
                throw new SparkException("Could not parse Master URL: '" + master + "'")
        }
    }
}

/**
  * A class encapsulating how to convert some type T to Writable. It stores both the Writable class
  * corresponding to T (e.g. IntWritable for Int) and a function for doing the conversion.
  * The getter for the writable class takes a ClassTag[T] in case this is a generic object
  * that doesn't know the type of T when it is created. This sounds strange but is necessary to
  * support converting subclasses of Writable to themselves (writableWritableConverter).
  */
private[spark] class WritableConverter[T](
                                                 val writableClass: ClassTag[T] => Class[_ <: Writable],
                                                 val convert: Writable => T)
        extends Serializable

object WritableConverter {

    // Helper objects for converting common types to Writable
    private[spark] def simpleWritableConverter[T, W <: Writable : ClassTag](convert: W => T)
    : WritableConverter[T] = {
        val wClass = classTag[W].runtimeClass.asInstanceOf[Class[W]]
        new WritableConverter[T](_ => wClass, x => convert(x.asInstanceOf[W]))
    }

    // The following implicit functions were in SparkContext before 1.3 and users had to
    // `import SparkContext._` to enable them. Now we move them here to make the compiler find
    // them automatically. However, we still keep the old functions in SparkContext for backward
    // compatibility and forward to the following functions directly.

    implicit def intWritableConverter(): WritableConverter[Int] =
        simpleWritableConverter[Int, IntWritable](_.get)

    implicit def longWritableConverter(): WritableConverter[Long] =
        simpleWritableConverter[Long, LongWritable](_.get)

    implicit def doubleWritableConverter(): WritableConverter[Double] =
        simpleWritableConverter[Double, DoubleWritable](_.get)

    implicit def floatWritableConverter(): WritableConverter[Float] =
        simpleWritableConverter[Float, FloatWritable](_.get)

    implicit def booleanWritableConverter(): WritableConverter[Boolean] =
        simpleWritableConverter[Boolean, BooleanWritable](_.get)

    implicit def bytesWritableConverter(): WritableConverter[Array[Byte]] = {
        simpleWritableConverter[Array[Byte], BytesWritable] { bw =>
            // getBytes method returns array which is longer then data to be returned
            Arrays.copyOfRange(bw.getBytes, 0, bw.getLength)
        }
    }

    implicit def stringWritableConverter(): WritableConverter[String] =
        simpleWritableConverter[String, Text](_.toString)

    implicit def writableWritableConverter[T <: Writable](): WritableConverter[T] =
        new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])
}

/**
  * A class encapsulating how to convert some type T to Writable. It stores both the Writable class
  * corresponding to T (e.g. IntWritable for Int) and a function for doing the conversion.
  * The Writable class will be used in `SequenceFileRDDFunctions`.
  */
private[spark] class WritableFactory[T](
                                               val writableClass: ClassTag[T] => Class[_ <: Writable],
                                               val convert: T => Writable) extends Serializable

object WritableFactory {

    private[spark] def simpleWritableFactory[T: ClassTag, W <: Writable : ClassTag](convert: T => W)
    : WritableFactory[T] = {
        val writableClass = implicitly[ClassTag[W]].runtimeClass.asInstanceOf[Class[W]]
        new WritableFactory[T](_ => writableClass, convert)
    }

    implicit def intWritableFactory: WritableFactory[Int] =
        simpleWritableFactory(new IntWritable(_))

    implicit def longWritableFactory: WritableFactory[Long] =
        simpleWritableFactory(new LongWritable(_))

    implicit def floatWritableFactory: WritableFactory[Float] =
        simpleWritableFactory(new FloatWritable(_))

    implicit def doubleWritableFactory: WritableFactory[Double] =
        simpleWritableFactory(new DoubleWritable(_))

    implicit def booleanWritableFactory: WritableFactory[Boolean] =
        simpleWritableFactory(new BooleanWritable(_))

    implicit def bytesWritableFactory: WritableFactory[Array[Byte]] =
        simpleWritableFactory(new BytesWritable(_))

    implicit def stringWritableFactory: WritableFactory[String] =
        simpleWritableFactory(new Text(_))

    implicit def writableWritableFactory[T <: Writable : ClassTag]: WritableFactory[T] =
        simpleWritableFactory(w => w)

}
