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

package org.apache.spark.deploy.client

import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.remote.{AssociationErrorEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.{ActorLogReceive, Utils, AkkaUtils}

/**
  * Interface allowing applications to speak with a Spark deploy cluster. Takes a master URL,
  * an app description, and a listener for cluster events, and calls back the listener when various
  * events occur.
  *
  * @param masterUrls Each url should look like spark://host:port.
  */
/*
*
* 所谓“注册”，就是同步一些元数据结构，保持集群的一致性，一般没有什么线程启动之类的额外操作
* */
private[spark] class AppClient(
                                      actorSystem: ActorSystem,
                                      masterUrls: Array[String],
                                      appDescription: ApplicationDescription,
                                      listener: AppClientListener,
                                      conf: SparkConf)
        extends Logging {

    val masterAkkaUrls = masterUrls.map(Master.toAkkaUrl(_, AkkaUtils.protocol(actorSystem)))

    val REGISTRATION_TIMEOUT = 20.seconds
    val REGISTRATION_RETRIES = 3

    var masterAddress: Address = null
    var actor: ActorRef = null
    var appId: String = null
    var registered = false
    var activeMasterUrl: String = null

    class ClientActor extends Actor with ActorLogReceive with Logging {
        var master: ActorSelection = null
        var alreadyDisconnected = false // To avoid calling listener.disconnected() multiple times
        var alreadyDead = false // To avoid calling listener.dead() multiple times
        var registrationRetryTimer: Option[Cancellable] = None

        //////////////////////////////////////////以下三个函数就是向masteractor注册application//////////////////////////////////////////////////
        /*
        *
        * 前期准备工作就是向CM注册
        * */
        override def preStart() {
            context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
            try {
                registerWithMaster()
            } catch {
                case e: Exception =>
                    logWarning("Failed to connect to master", e)
                    markDisconnected()
                    context.stop(self)
            }
        }

        /*
        *
        * 向所有master注册AppDescription，和Master.scala文件中的函数类似
        * */
        def tryRegisterAllMasters() {
            //  master可能又多个url，HA模式的话，会有多个URL，所以要分别注册连接application
            //  但是同时只有一个master是alive状态，所有也只能收到一个master的注册成功消息
            for (masterAkkaUrl <- masterAkkaUrls) {
                logInfo("Connecting to master " + masterAkkaUrl + "...")
                //引用master actor发送注册application信息，不是driver，是actor
                //  master是CM，集群管理器，可别和什么driver混淆了
                //  先通过actorSelection找到mater的actor
                /*
                *
                * actorselection（）方法会返回Actorselection选择路径，而不会返回ActorRef引用。使用Actorselection对象可以向该路径指向的Actor对象发送消息。
                * 然而，请注意，与使用ActorRef引用的方式相比，通过这种方式发送消息的速度较慢并且会占用更多资源。
                * 但是，actorselection（）方法仍旧是一个优秀的工具，因为它可以执行查询由通配符代表的多个Actor对象的操作，从而使你能够向Actorselection选择路径指向的任意个Actor对象广播消息。
                * */
                //  根据masterUrl查找现有的master的actor，不会创建新的actor
                //  Question：通过哪个actorSystem找的actor？？？？？？？AppClient还是Master的？？？？？？？或者根本不在乎哪个actorSystem，只要是通过url查找就好了
                //  但不管怎么说，从事实上看，RegisterApplication消息接收方是Master这个actor
                val actor = context.actorSelection(masterAkkaUrl)
                actor ! RegisterApplication(appDescription)
            }
        }

        /*
        * 向CM注册application
        * */
        def registerWithMaster() {
            tryRegisterAllMasters()
            import context.dispatcher
            var retries = 0
            registrationRetryTimer = Some {
                context.system.scheduler.schedule(REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT) {
                    Utils.tryOrExit {
                        retries += 1
                        if (registered) {
                            registrationRetryTimer.foreach(_.cancel())
                            /*
                            * 连接CM三次失败就放弃
                            * */
                        } else if (retries >= REGISTRATION_RETRIES) {
                            markDead("All masters are unresponsive! Giving up.")
                        } else {
                            tryRegisterAllMasters()
                        }
                    }
                }
            }
        }
        /////////////////////////////////////////以上三个函数是注册application///////////////////////////////////////////////////

        def changeMaster(url: String) {
            // activeMasterUrl is a valid Spark url since we receive it from master.
            //  activeMasterUrl是可用spark url，因为我们是从Master的注册成功返回消息得到的masterUrl
            activeMasterUrl = url
            master = context.actorSelection(
                Master.toAkkaUrl(activeMasterUrl, AkkaUtils.protocol(actorSystem)))
            masterAddress = Master.toAkkaAddress(activeMasterUrl, AkkaUtils.protocol(actorSystem))
        }

        private def isPossibleMaster(remoteUrl: Address) = {
            masterAkkaUrls.map(AddressFromURIString(_).hostPort).contains(remoteUrl.hostPort)
        }

        override def receiveWithLogging = {
            case RegisteredApplication(appId_, masterUrl) =>
                //  在Master中给该application生成id后，返回注册成功消息，该ClientActor才得知自己提交的application的id号是多少！！！！！！！！！
                appId = appId_
                registered = true
                changeMaster(masterUrl)
                listener.connected(appId)

            case ApplicationRemoved(message) =>
                markDead("Master removed our application: %s".format(message))
                context.stop(self)

            case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
                val fullId = appId + "/" + id
                logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort,
                    cores))
                master ! ExecutorStateChanged(appId, id, ExecutorState.RUNNING, None, None)
                listener.executorAdded(fullId, workerId, hostPort, cores, memory)

            case ExecutorUpdated(id, state, message, exitStatus) =>
                val fullId = appId + "/" + id
                val messageText = message.map(s => " (" + s + ")").getOrElse("")
                logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
                if (ExecutorState.isFinished(state)) {
                    listener.executorRemoved(fullId, message.getOrElse(""), exitStatus)
                }

            case MasterChanged(masterUrl, masterWebUiUrl) =>
                logInfo("Master has changed, new master is at " + masterUrl)
                changeMaster(masterUrl)
                alreadyDisconnected = false
                sender ! MasterChangeAcknowledged(appId)

            case DisassociatedEvent(_, address, _) if address == masterAddress =>
                logWarning(s"Connection to $address failed; waiting for master to reconnect...")
                markDisconnected()

            case AssociationErrorEvent(cause, _, address, _, _) if isPossibleMaster(address) =>
                logWarning(s"Could not connect to $address: $cause")

                //  关闭ClientActor
            case StopAppClient =>
                markDead("Application has been stopped.")
                sender ! true
                context.stop(self)
        }

        /**
          * Notify the listener that we disconnected, if we hadn't already done so before.
          */
        def markDisconnected() {
            if (!alreadyDisconnected) {
                listener.disconnected()
                alreadyDisconnected = true
            }
        }

        def markDead(reason: String) {
            if (!alreadyDead) {
                listener.dead(reason)
                alreadyDead = true
            }
        }

        override def postStop() {
            registrationRetryTimer.foreach(_.cancel())
        }

    }

    def start() {
        // Just launch an actor; it will call back into the listener.
        /*
        *石锤了，这句话就是启动actor的意思。
        * */
        actor = actorSystem.actorOf(Props(new ClientActor))
    }

    def stop() {
        if (actor != null) {
            try {
                val timeout = AkkaUtils.askTimeout(conf)
                val future = actor.ask(StopAppClient)(timeout)
                Await.result(future, timeout)
            } catch {
                case e: TimeoutException =>
                    logInfo("Stop request to Master timed out; it may already be shut down.")
            }
            actor = null
        }
    }
}
