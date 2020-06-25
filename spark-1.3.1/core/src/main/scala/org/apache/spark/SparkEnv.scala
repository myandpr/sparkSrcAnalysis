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

import java.io.File
import java.net.Socket

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Properties

import akka.actor._
import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.scheduler.{OutputCommitCoordinator, LiveListenerBus}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorActor
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleMemoryManager, ShuffleManager}
import org.apache.spark.storage._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
  * :: DeveloperApi ::
  * Holds all the runtime environment objects for a running Spark instance (either master or worker),
  * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
  * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
  * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
  *
  * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
  * in a future release.
  */
@DeveloperApi
class SparkEnv(
                      val executorId: String,
                      val actorSystem: ActorSystem,
                      val serializer: Serializer,
                      val closureSerializer: Serializer,
                      val cacheManager: CacheManager,
                      val mapOutputTracker: MapOutputTracker,
                      val shuffleManager: ShuffleManager,
                      val broadcastManager: BroadcastManager,
                      val blockTransferService: BlockTransferService,
                      val blockManager: BlockManager,
                      val securityManager: SecurityManager,
                      val httpFileServer: HttpFileServer,
                      val sparkFilesDir: String,
                      val metricsSystem: MetricsSystem,
                      val shuffleMemoryManager: ShuffleMemoryManager,
                      val outputCommitCoordinator: OutputCommitCoordinator,
                      val conf: SparkConf) extends Logging {

    private[spark] var isStopped = false
    private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

    // A general, soft-reference map for metadata needed during HadoopRDD split computation
    // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
    private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

    private[spark] def stop() {
        isStopped = true
        pythonWorkers.foreach { case (key, worker) => worker.stop() }
        Option(httpFileServer).foreach(_.stop())
        mapOutputTracker.stop()
        shuffleManager.stop()
        broadcastManager.stop()
        blockManager.stop()
        blockManager.master.stop()
        metricsSystem.stop()
        outputCommitCoordinator.stop()
        actorSystem.shutdown()
        // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
        // down, but let's call it anyway in case it gets fixed in a later release
        // UPDATE: In Akka 2.1.x, this hangs if there are remote actors, so we can't call it.
        // actorSystem.awaitTermination()

        // Note that blockTransferService is stopped by BlockManager since it is started by it.
    }

    private[spark]
    def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
        synchronized {
            val key = (pythonExec, envVars)
            pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
        }
    }

    private[spark]
    def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
        synchronized {
            val key = (pythonExec, envVars)
            pythonWorkers.get(key).foreach(_.stopWorker(worker))
        }
    }

    private[spark]
    def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
        synchronized {
            val key = (pythonExec, envVars)
            pythonWorkers.get(key).foreach(_.releaseWorker(worker))
        }
    }
}
/////////////////////////////////////////////class和object的分割线////////////////////////////////////////////////////
/*
*
* 重要的是建立SparkEnv的actorSystem
* */
object SparkEnv extends Logging {
    @volatile private var env: SparkEnv = _

    private[spark] val driverActorSystemName = "sparkDriver"
    private[spark] val executorActorSystemName = "sparkExecutor"

    def set(e: SparkEnv) {
        env = e
    }

    /**
      * Returns the SparkEnv.
      */
    def get: SparkEnv = {
        env
    }

    /**
      * Returns the ThreadLocal SparkEnv.
      */
    @deprecated("Use SparkEnv.get instead", "1.2")
    def getThreadLocal: SparkEnv = {
        env
    }

    /**
      * Create a SparkEnv for the driver.
      */
    private[spark] def createDriverEnv(
                                              conf: SparkConf,
                                              isLocal: Boolean,
                                              listenerBus: LiveListenerBus,
                                              mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
        assert(conf.contains("spark.driver.host"), "spark.driver.host is not set on the driver!")
        assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
        val hostname = conf.get("spark.driver.host")
        val port = conf.get("spark.driver.port").toInt
        create(
            conf,
            SparkContext.DRIVER_IDENTIFIER,
            hostname,
            port,
            isDriver = true,
            isLocal = isLocal,
            listenerBus = listenerBus,
            mockOutputCommitCoordinator = mockOutputCommitCoordinator
        )
    }

    /**
      * Create a SparkEnv for an executor.
      * In coarse-grained mode, the executor provides an actor system that is already instantiated.
      */
    private[spark] def createExecutorEnv(
                                                conf: SparkConf,
                                                executorId: String,
                                                hostname: String,
                                                port: Int,
                                                numCores: Int,
                                                isLocal: Boolean): SparkEnv = {
        val env = create(
            conf,
            executorId,
            hostname,
            port,
            isDriver = false,
            isLocal = isLocal,
            numUsableCores = numCores
        )
        SparkEnv.set(env)
        env
    }

    /**
      * Helper method to create a SparkEnv for a driver or an executor.
      * 有两种SparkEnv：1、driver SparkEnv；2、executor SparkEnv
      * 创建SparkEnv的具体过程，同时创建了很多东西
      */
    private def create(
                              conf: SparkConf,
                              executorId: String,
                              hostname: String,
                              port: Int,
                              isDriver: Boolean,
                              isLocal: Boolean,
                              listenerBus: LiveListenerBus = null,
                              numUsableCores: Int = 0,
                              mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

        // Listener bus is only used on the driver
        if (isDriver) {
            assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
        }

        /*
        * 创建securityManager
        * */
        val securityManager = new SecurityManager(conf)

        // Create the ActorSystem for Akka and get the port it binds to.
        /*
        *
        * 如果是isDriver，则创建driverActorSystemName，否则创建executorActorSystemName
        * 要明白：不管是SparkContext这儿的driver端，还是executor端，都会创建自己的SparkEnv的actorSystem。
        * */
        val (actorSystem, boundPort) = {
            val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
            AkkaUtils.createActorSystem(actorSystemName, hostname, port, conf, securityManager)
        }

        // Figure out which port Akka actually bound to in case the original port is 0 or occupied.
        if (isDriver) {
            conf.set("spark.driver.port", boundPort.toString)
        } else {
            conf.set("spark.executor.port", boundPort.toString)
        }

        // Create an instance of the class with the given name, possibly initializing it with our conf
        /*
        *
        *不熟悉java的Class.forName方法用法。
        * */
        def instantiateClass[T](className: String): T = {
            /*
            * 反射机制的使用
            * 这里就是ClassLoader类加载器的使用原理
            * */
            val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
            // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
            // SparkConf, then one taking no arguments
            try {
                cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
                        .newInstance(conf, new java.lang.Boolean(isDriver))
                        .asInstanceOf[T]
            } catch {
                case _: NoSuchMethodException =>
                    try {
                        cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
                    } catch {
                        case _: NoSuchMethodException =>
                            cls.getConstructor().newInstance().asInstanceOf[T]
                    }
            }
        }

        // Create an instance of the class named by the given SparkConf property, or defaultClassName
        // if the property is not set, possibly initializing it with our conf
        def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
            instantiateClass[T](conf.get(propertyName, defaultClassName))
        }

        /*
        *
        * 貌似是根据propertyName: String来创建对应的类，初始化序列化对象？？？
        * */
        val serializer = instantiateClassFromConf[Serializer](
            "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        logDebug(s"Using serializer: ${serializer.getClass}")

        val closureSerializer = instantiateClassFromConf[Serializer](
            "spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer")

        /*
        *
        *
        * newActor:=>Actor语法不清楚！！！！
        * 该函数无非就是在ActorSystem中开始Actor处理消息，返回actor
        * 开始执行actor监听处理
        * 如果是driver端，就注册启动driver的actor
        * 如果是executor端，就发现driver的actor
        * 字面意思理解：registerOrLookup就是注册或寻找
        * */
        //  返回newActor的引用
        def registerOrLookup(name: String, newActor: => Actor): ActorRef = {
            if (isDriver) {
                logInfo("Registering " + name)
                /*
                * 注册启动newActor并返回actor引用
                * */
                actorSystem.actorOf(Props(newActor), name = name)
            } else {
                /*
                *
                * 如果是executor启动actor，则需要查找driver端的actor，用来给driver发消息
                * */
                AkkaUtils.makeDriverRef(name, conf, actorSystem)
            }
        }

        /*
        *
        * 根据SparkContext创建SparkEnv时传入isDriver参数，选择创建对应的MapOutputTrackerMaster或MapOutputTrackerWorker
        * 管理shuffle的结果存储管理
        * */
        val mapOutputTracker = if (isDriver) {
            new MapOutputTrackerMaster(conf)
        } else {
            new MapOutputTrackerWorker(conf)
        }

        // Have to assign trackerActor after initialization as MapOutputTrackerActor
        // requires the MapOutputTracker itself
        /*
        *
        * registerOrLookup函数返回了actor，是driver的actor，not executor的actor
        *
        * 这个地方才是给trackerActor第一次赋值，赋值的是driver端的actor
        * */
        //  不管是MapOutputTrackerMaster还是MapOutputTrackerWorker，它的内部变量trackerActor都需要保存MapOutputTrackerMasterActor，以方便通信
        mapOutputTracker.trackerActor = registerOrLookup(
            "MapOutputTracker",
            /*
            *
            * MapOutputTrackerMasterActor类型的actor
            * */
            new MapOutputTrackerMasterActor(mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

        // Let the user specify short names for shuffle managers
        /*
        *
        * 缩写
        * 两种shuffleManager：HashShuffleManager，SortShuffleManager
        * */
        val shortShuffleMgrNames = Map(
            "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
            "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager")
        /*
        * spark-1.3.1版本默认shuffle是SortShuffle
        * 获取spark集群配置的默认shuffle方法
        * */
        val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
        val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
        /*
        * 反射的应用
        * */
        val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

        /*
        * 创建ShuffleMemoryManager
        * blockTransferService默认为NettyBlockTransferService ,它使用Netty法人一步时间驱动 的网络应用框架，提供web服务及客户端，获取远程节点上的Block集合。
        * */
        val shuffleMemoryManager = new ShuffleMemoryManager(conf)

        /*
        * 创建块传输服务：blockTransferService，分为两种：1、netty；2、nio
        * 就是负责数据传输的
        * 是个开源框架
        * */
        val blockTransferService =
            conf.get("spark.shuffle.blockTransferService", "netty").toLowerCase match {
                case "netty" =>
                    new NettyBlockTransferService(conf, securityManager, numUsableCores)
                case "nio" =>
                    new NioBlockTransferService(conf, securityManager)
            }

        /*
        *
        * a) BlockManagerMaster：对整个集群的Block数据进行管理的；
        * b) MapOutputTracker:跟踪所有mapper的输出的；
        *
        * */


        /*
        * Driver端和executor端的SparkEnv都定义了BlockManagerMaster
        * 参数isDriver仅仅是当SparkContext调用stop()函数关闭driver端所有组件时调用SparkEnv.stop()调用BlockManagerMaster.stop()时判断用的。
        * 意味着只有driver端的BlockManagerMaster才可以执行操作stop(),executor端的BlockManagerMaster端收到stop信号后不处理。
        * */
        //  每启动一个ExecutorBackend都会实例化BlockManager并通过远程通讯的方式注册给BlockManagerMaster；实质上是Executor中的BlockManager在启动的时候注册给了Driver上的BlockManagerMasterEndpoint；
        //  registerOrLookup返回的是一个driverActor，其实就是启动一个BlockManagerMasterActor，
        // 笼统的讲，driverActor和BlockManagerMasterActor是一样的，这一点有待于确认，但八九不离十
        //  registerOrLookup返回的是BlockManagerMasterActor的引用
        //  石锤了，BlockManagerMaster的参数driverActor就是BlockManagerMasterActor引用，因为registerOrLookup返回值就是BlockManagerMasterActor引用
        val blockManagerMaster = new BlockManagerMaster(registerOrLookup(
            "BlockManagerMaster",
            new BlockManagerMasterActor(isLocal, conf, listenerBus)), conf, isDriver)

        // NB: blockManager is not valid until initialize() is called later.
        /*
        * 用到actorSystem、BlockManagerMaster（这是个actor）、mapOutputTracker、shuffleManager、blockTransferService、securityManager参数，创建BlockManager，
        * */
        //  actorSystem是不同的，executor和driver的SparkEnv，是两个，各是各的
        //  本质上：BlockManager持有BlockManagerMaster参数，而BlockManagerMaster持有BlockManagerMasterActor参数，这样就可以通信了
        //  而MapOutputTracker本质上：MapOutputTrackerMaster和MapOutputTrackerWorker都持有trackerActor变量，也就是MapOutputTrackerMasterActor，
        //  而MapOutputTrackerMasterActor持有MapOutputTrackerMaster参数
        val blockManager = new BlockManager(executorId, actorSystem, blockManagerMaster,
            serializer, conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager,
            numUsableCores)


        /*
        * 创建broadcastManager广播变量管理器，是建立在securityManager上的
        * */
        val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

        /*
        *
        * 创建CacheManager缓存管理器，CacheManager是建立在BlockManager基础上的
        * */
        val cacheManager = new CacheManager(blockManager)

        /*
        * 创建http服务器并初始化，在初始化中设置好jar、file等在driver端的目录，然后启动服务httpServer.start()
        * 只运行在driver端
        * 用来在spark-submit提交application时的-jar和-file参数分发到各个worker的过程
        * */
        val httpFileServer =
            if (isDriver) {
                val fileServerPort = conf.getInt("spark.fileserver.port", 0)
                val server = new HttpFileServer(conf, securityManager, fileServerPort)
                server.initialize()
                conf.set("spark.fileserver.uri", server.serverUri)
                server
            } else {
                null
            }

        /*
        * 创建监控系统，但是不会现在启动，只有在taskscheduler给我们appId后才会真正启动这个系统，
        * SparkContext.scala文件的，589行
        * 也是分为driver和executor两种，executor的当下启动，driver的 在TaskScheduler.start()之后启动
        * */
        val metricsSystem = if (isDriver) {
            // Don't start metrics system right now for Driver.
            // We need to wait for the task scheduler to give us an app ID.
            // Then we can start the metrics system.
            MetricsSystem.createMetricsSystem("driver", conf, securityManager)
        } else {
            // We need to set the executor ID before the MetricsSystem is created because sources and
            // sinks specified in the metrics configuration file will want to incorporate this executor's
            // ID into the metrics they report.
            conf.set("spark.executor.id", executorId)
            val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
            ms.start()
            ms
        }

        // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
        // this is a temporary directory; in distributed mode, this is the executor's current working
        // directory.
        /*
        * 本地模式是一个临时目录；分布式模式是executor的当前工作目录
        * SparkContext.addFile()下载的file，都存在以这个为根目录的路径下
        *
        * 设置sparkFiles目录，用来下载依赖。
        * */
        val sparkFilesDir: String = if (isDriver) {
            Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
        } else {
            "."
        }

        // Warn about deprecated spark.cache.class property
        if (conf.contains("spark.cache.class")) {
            logWarning("The spark.cache.class property is no longer being used! Specify storage " +
                    "levels using the RDD.persist() method instead.")
        }

        /*
        *
        * 不清楚该方法的作用
        * 反正是创建了某个功能的actor
        * */
        val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
            new OutputCommitCoordinator(conf)
        }
        val outputCommitCoordinatorActor = registerOrLookup("OutputCommitCoordinator",
            new OutputCommitCoordinatorActor(outputCommitCoordinator))
        outputCommitCoordinator.coordinatorActor = Some(outputCommitCoordinatorActor)

        /*
        *
        * 这种用法很奇怪，在SparkEnv class里new SparkEnv，返回SparkEnv对象
        * 不奇怪，因为这是在SparkEnv的伴生对象里创建的SparkEnv对象！！
        * */
        new SparkEnv(
            executorId,
            actorSystem,
            serializer,
            closureSerializer,
            cacheManager,
            mapOutputTracker,
            shuffleManager,
            broadcastManager,
            blockTransferService,
            blockManager,
            securityManager,
            httpFileServer,
            sparkFilesDir,
            metricsSystem,
            shuffleMemoryManager,
            outputCommitCoordinator,
            conf)
    }

    /**
      * Return a map representation of jvm information, Spark properties, system properties, and
      * class paths. Map keys define the category, and map values represent the corresponding
      * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
      */
    /*
    * 返回各种属性的相关环境变量的key-value格式Map
    * jvm information, Spark properties, system properties, and class paths
    * .
    * */
    private[spark]
    def environmentDetails(
                                  conf: SparkConf,
                                  schedulingMode: String,
                                  addedJars: Seq[String],
                                  addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

        import Properties._
        val jvmInformation = Seq(
            ("Java Version", s"$javaVersion ($javaVendor)"),
            ("Java Home", javaHome),
            ("Scala Version", versionString)
        ).sorted

        // Spark properties
        // This includes the scheduling mode whether or not it is configured (used by SparkUI)
        val schedulerMode =
        if (!conf.contains("spark.scheduler.mode")) {
            Seq(("spark.scheduler.mode", schedulingMode))
        } else {
            Seq[(String, String)]()
        }
        val sparkProperties = (conf.getAll ++ schedulerMode).sorted

        // System properties that are not java classpaths
        val systemProperties = Utils.getSystemProperties.toSeq
        val otherProperties = systemProperties.filter { case (k, _) =>
            /*
            * 过滤jvm、spark相关的环境变量
            * */
            k != "java.class.path" && !k.startsWith("spark.")
        }.sorted

        // Class paths including all added jars and files
        val classPathEntries = javaClassPath
                .split(File.pathSeparator)
                .filterNot(_.isEmpty)
                .map((_, "System Classpath"))
        val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
        val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

        Map[String, Seq[(String, String)]](
            "JVM Information" -> jvmInformation,
            "Spark Properties" -> sparkProperties,
            "System Properties" -> otherProperties,
            "Classpath Entries" -> classPaths)
    }
}
