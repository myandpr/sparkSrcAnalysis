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

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import scala.collection.mutable
import scala.concurrent.Await

import akka.actor.{Actor, ActorSelection, Props}
import akka.pattern.Patterns
import akka.remote.{RemotingLifecycleEvent, DisassociatedEvent}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, SignalLogger, Utils}

//  CoarseGrainedExecutorBackend本身就是一个Actor，但是CoarseGrainedSchedulerBackend本身不是Actor，它内部有个DriverActor变量
private[spark] class CoarseGrainedExecutorBackend(
                                                         driverUrl: String,
                                                         executorId: String,
                                                         hostPort: String,
                                                         cores: Int,
                                                         userClassPath: Seq[URL],
                                                         env: SparkEnv)
        extends Actor with ActorLogReceive with ExecutorBackend with Logging {

    Utils.checkHostPort(hostPort, "Expected hostport")

    /*
    *
    * 需要记住：CoarseGrainedExecutorBackend和executor是一一对应的，1:1，所以CSDN一些博客通常都讲CoarseGrainedExecutorBackend就是executor，是一回事
    * */
    var executor: Executor = null
    var driver: ActorSelection = null

    //  CoarseGrainedExecutorBackend只要一启动就寻找driverurl，向driverActor注册，将自己的信息（executorId，hostPort，cores等）发送给DriverActor，
    override def preStart() {
        logInfo("Connecting to driver: " + driverUrl)
        //  context是actor自身的所有上下文环境，所有变量
        //  重要：该driver就是CoarseGrainedSchedulerBackend的DriverActor！！！！！！！！！！
        driver = context.actorSelection(driverUrl)
        driver ! RegisterExecutor(executorId, hostPort, cores, extractLogUrls)
        context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    def extractLogUrls: Map[String, String] = {
        val prefix = "SPARK_LOG_URL_"
        sys.env.filterKeys(_.startsWith(prefix))
                .map(e => (e._1.substring(prefix.length).toLowerCase, e._2))
    }

    override def receiveWithLogging = {
        //  只有收到CoarseGrainedSchedulerBackend的DriverActor返回的注册成功的消息，才会创建Executor实例
        case RegisteredExecutor =>
            logInfo("Successfully registered with driver")
            val (hostname, _) = Utils.parseHostPort(hostPort)
            //  为什么说CoarseGrainedExecutorBackend就是executor呢？？？？？
            //  因为CoarseGrainedExecutorBackend内有executor实例变量，在向DriverActor注册时候就实例化了，之后启动task的时候（如下面的LaunchTask消息）就会调用自身的executor实例运行task
            //  注意：创建实例时，就已经启动了线程池，val threadPool = Utils.newDaemonCachedThreadPool("Executor task launch worker")
            executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)

            // 此处不应该给CoarseGrainedSchedulerBackend的DriverActor回复“executor启动成功”的消息么？就像TCP三次握手那样？？？？？？？？？？？？
            //  否则，executor没有启动成功，但是CoarseGrainedSchedulerBackend里的ExecutorDataMap中的executor数据就不同步了

        case RegisterExecutorFailed(message) =>
            logError("Slave registration failed: " + message)
            System.exit(1)

            /*
            * 之所以该executor收到LaunchTask消息，是因为CoarseGrainedSchedulerBackend已经知道了该task该发送到哪个该executor上，所以才发送过来了
            *
            * 对于executor来说，只有LaunchTask，并没有LaunchTasks，因为只能一个一个的执行task，不能批量执行，批量的话，在CoarseGrainedSchedulerBackend中有，然后一个个的发送给CoarseGrainedExecutorBackend
            * */
        case LaunchTask(data) =>
            //  什么时候才会发生executor == null的情况呢？
            //  当CoarseGrainedSchedulerBackend的DriverActor回复RegisteredExecutor注册成功消息后，74行的executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)由于各种原因，比如内存不足等，失败了
            if (executor == null) {
                logError("Received LaunchTask command but executor was null")
                System.exit(1)
            } else {
                val ser = env.closureSerializer.newInstance()
                //  反序列化task，拿到了TaskDescription类型的task，获得了taskid和executorid的关系
                val taskDesc = ser.deserialize[TaskDescription](data.value)
                logInfo("Got assigned（分配的） task " + taskDesc.taskId)
                //  其中的taskDesc.serializedTask是序列化的Task，包含了JarName（jarName: 这里的JarName是网络文件名：spark://192.168.121.101:37684/jars/spark-examples_2.11-2.1.0.jar）
                //  timestamp等等，如果timestamp是新的，则需要重新fetch
                //  Driver所运行的class等包括依赖的Jar文件在Executor上并不存在，Executor首先要fetch所依赖的jars，也就是TaskDescription中serializedTask中的jar部分
                //  在LaunchTask里的消息中并不携带Jar的内容，用到的时候，才会根据jar的网络路径去下载
                executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
                    taskDesc.name, taskDesc.serializedTask)
            }

        case KillTask(taskId, _, interruptThread) =>
            //  如果需要kill的task所在的CoarseGrainedExecutorBackend的executor实例已经停止服务stop了，就直接退出不用执行什么操作
            if (executor == null) {
                logError("Received KillTask command but executor was null")
                System.exit(1)
            } else {
                //在executor级别kill该task
                executor.killTask(taskId, interruptThread)
            }

        case x: DisassociatedEvent =>
            if (x.remoteAddress == driver.anchorPath.address) {
                logError(s"Driver $x disassociated! Shutting down.")
                System.exit(1)
            } else {
                logWarning(s"Received irrelevant DisassociatedEvent $x")
            }

        case StopExecutor =>
            logInfo("Driver commanded a shutdown")
            //  关闭executor的actor、线程pool
            executor.stop()
            //  关闭Actor系统
            context.stop(self)
            context.system.shutdown()
    }

    override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
        driver ! StatusUpdate(executorId, taskId, state, data)
    }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {

    private def run(
                           driverUrl: String,
                           executorId: String,
                           hostname: String,
                           cores: Int,
                           appId: String,
                           workerUrl: Option[String],
                           userClassPath: Seq[URL]) {

        SignalLogger.register(log)

        SparkHadoopUtil.get.runAsSparkUser { () =>
            // Debug code
            Utils.checkHost(hostname)

            // Bootstrap to fetch the driver's Spark properties.
            val executorConf = new SparkConf
            val port = executorConf.getInt("spark.executor.port", 0)

            //  创建actorSystem
            val (fetcher, _) = AkkaUtils.createActorSystem(
                "driverPropsFetcher",
                hostname,
                port,
                executorConf,
                new SecurityManager(executorConf))

            //  这里不是创建driver端的Actor，而是查找！！！！！！根据driverUrl查找到对应的actor
            val driver = fetcher.actorSelection(driverUrl)
            val timeout = AkkaUtils.askTimeout(executorConf)
            val fut = Patterns.ask(driver, RetrieveSparkProps, timeout)
            val props = Await.result(fut, timeout).asInstanceOf[Seq[(String, String)]] ++
                    Seq[(String, String)](("spark.app.id", appId))
            fetcher.shutdown()

            // Create SparkEnv using properties we fetched from the driver.
            val driverConf = new SparkConf()
            for ((key, value) <- props) {
                // this is required for SSL in standalone mode
                if (SparkConf.isExecutorStartupConf(key)) {
                    driverConf.setIfMissing(key, value)
                } else {
                    driverConf.set(key, value)
                }
            }

            //  创建executor端的env，SparkEnv.createExecutorEnv
            val env = SparkEnv.createExecutorEnv(
                driverConf, executorId, hostname, port, cores, isLocal = false)

            // SparkEnv sets spark.driver.port so it shouldn't be 0 anymore.
            val boundPort = env.conf.getInt("spark.executor.port", 0)
            assert(boundPort != 0)

            // Start the CoarseGrainedExecutorBackend actor.
            //  这一步才是启动CoarseGrainedExecutorBackend actor.，之前class CoarseGrainedExecutorBackend都是定义
            val sparkHostPort = hostname + ":" + boundPort
            env.actorSystem.actorOf(
                Props(classOf[CoarseGrainedExecutorBackend],
                    driverUrl, executorId, sparkHostPort, cores, userClassPath, env),
                name = "Executor")
            workerUrl.foreach { url =>
                env.actorSystem.actorOf(Props(classOf[WorkerWatcher], url), name = "WorkerWatcher")
            }
            env.actorSystem.awaitTermination()
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 特别重要(CoarseGrainedExecutorBackend和executor之间关系)：https://www.jianshu.com/p/9a224669097c            ///
    //  应该是一一对应的，想一想在spark集群里的不同worker节点中，jps一下，就能看到                                       ///
    //  CoarseGrainedSchedulerBackend无main函数，CoarseGrainedExecutorBackend有main函数，是因为它们启动的方式不一样， ///
    //  scheduler是函数调用启动的，executor的是通过Command直接启动的                                                 ///
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //  有main方法。通过main启动CoarseGrainedExecutorBackend
    //  这里有个疑问，启动几个executor，则需要启动几个CoarseGrainedExecutorBackend？？？？还是只需要启动一个CoarseGrainedExecutorBackend，它负责所有的executor的启动？？？？？
    //  不是的，每个CoarseGrainedExecutorBackend有一个executor，这个结论应该没问题
    def main(args: Array[String]) {
        var driverUrl: String = null
        var executorId: String = null
        var hostname: String = null
        var cores: Int = 0
        var appId: String = null
        var workerUrl: Option[String] = None
        val userClassPath = new mutable.ListBuffer[URL]()

        var argv = args.toList
        while (!argv.isEmpty) {
            argv match {
                case ("--driver-url") :: value :: tail =>
                    driverUrl = value
                    argv = tail
                case ("--executor-id") :: value :: tail =>
                    executorId = value
                    argv = tail
                case ("--hostname") :: value :: tail =>
                    hostname = value
                    argv = tail
                case ("--cores") :: value :: tail =>
                    cores = value.toInt
                    argv = tail
                case ("--app-id") :: value :: tail =>
                    appId = value
                    argv = tail
                case ("--worker-url") :: value :: tail =>
                    // Worker url is used in spark standalone mode to enforce fate-sharing with worker
                    workerUrl = Some(value)
                    argv = tail
                case ("--user-class-path") :: value :: tail =>
                    userClassPath += new URL(value)
                    argv = tail
                case Nil =>
                case tail =>
                    System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
                    printUsageAndExit()
            }
        }

        if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
                appId == null) {
            printUsageAndExit()
        }

        run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
    }

    private def printUsageAndExit() = {
        System.err.println(
            """
              |"Usage: CoarseGrainedExecutorBackend [options]
              |
              | Options are:
              |   --driver-url <driverUrl>
              |   --executor-id <executorId>
              |   --hostname <hostname>
              |   --cores <cores>
              |   --app-id <appid>
              |   --worker-url <workerUrl>
              |   --user-class-path <url>
              |""".stripMargin)
        System.exit(1)
    }

}
