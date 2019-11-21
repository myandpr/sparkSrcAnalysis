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

import java.io._
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.{HashSet, HashMap, Map}
import scala.concurrent.Await
import scala.collection.JavaConversions._

import akka.actor._
import akka.pattern.ask

import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

private[spark] sealed trait MapOutputTrackerMessage

private[spark] case class GetMapOutputStatuses(shuffleId: Int)
        extends MapOutputTrackerMessage

private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

/** Actor class for MapOutputTrackerMaster */
//  MapOutputTrackerMasterActor本身就是个actor
private[spark] class MapOutputTrackerMasterActor(tracker: MapOutputTrackerMaster, conf: SparkConf)
        extends Actor with ActorLogReceive with Logging {
    val maxAkkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)

    //  有个疑问：这两个消息是哪里发送到这里的？？？？？？？？？是MapOutputTrackerWorker发送的吗？？？？？？？？？？？？？
    override def receiveWithLogging = {
        //  这个shuffleId指的是第几个shuffle算子，比如一个action中，进行了多次的join、reducByKey，则会出现多个shuffle，所以需要一个shuffleId来标记
        case GetMapOutputStatuses(shuffleId: Int) =>
            //  就是这个端口hostport发送的请求shuffle的map输出位置信息
            val hostPort = sender.path.address.hostPort
            logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
            /*
            * 最终是调用了MapOutputTrackerMaster的mapStatuses或cachedSerializedStatuses
            * 之所以要获得序列化后的数据，是因为要传输给sender，所以最好传输序列化的数据
            * */
            val mapOutputStatuses = tracker.getSerializedMapOutputStatuses(shuffleId)
            val serializedSize = mapOutputStatuses.size
            //  不太理解这个逻辑，如果数据大小超过了actor传输的最大值，就直接抛出异常不处理了？？？？？？难道不应该是至少保存到BlockManager之类的么？？？
            if (serializedSize > maxAkkaFrameSize) {
                val msg = s"Map output statuses were $serializedSize bytes which " +
                        s"exceeds spark.akka.frameSize ($maxAkkaFrameSize bytes)."

                /* For SPARK-1244 we'll opt for just logging an error and then throwing an exception.
                 * Note that on exception the actor will just restart. A bigger refactoring (SPARK-1239)
                 * will ultimately remove this entire code path. */
                val exception = new SparkException(msg)
                logError(msg, exception)
                throw exception
            }
            /*
            * 如果值大小在actor传输范围内，则返回给发送方actor
            * */
            sender ! mapOutputStatuses

            //  停止MapOutputTracker就是停止对应的actor，对于这里，就是停止本身这个actor
        case StopMapOutputTracker =>
            logInfo("MapOutputTrackerActor stopped!")
            sender ! true
            context.stop(self)
    }
}

/**
  * Class that keeps track of the location of the map output of
  * a stage. This is abstract because different versions of MapOutputTracker
  * (driver and executor) use different HashMap to store its metadata.
  */
/*
* 这个类保存所有的stage中map阶段的输出位置，之所以是抽象class，
* 是因为不同的MapOutputTracker（driver，executor）使用不同的HashMap存储metadata
* 下面两个class分别是MapOutputTrackerMaster和MapOutputTrackerWorker
* */
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {
    /*
    * akka的请求操作时间
    * */
    private val timeout = AkkaUtils.askTimeout(conf)
    private val retryAttempts = AkkaUtils.numRetries(conf)
    private val retryIntervalMs = AkkaUtils.retryWaitMs(conf)

    /** Set to the MapOutputTrackerActor living on the driver. */
    /*
    * 设置运行在driver端的MapOutputTrackerActor
    *
    * 看不到该trackerActor是如何设置的？？为什么只有使用，没有设置？？？
    *
    *
    * 该变量是public，意味着可以在外部的类中，对其设置。。。。。。。。。
    * 具体是在SparkEnv.scala中create函数实现中第一次赋值
    * */
    var trackerActor: ActorRef = _

    /**
      * This HashMap has different behavior for the driver and the executors.
      * 这个HashMap对driver和executors有不同的行为
      * 在driver上，这个HashMap作为ShuffleMapTasks的输出结果
      * 在executors上，这个HashMap简单的作为cache缓存服务，在这个缓存中，未命中的从driver的相应的HashMap中获取
      *
      * On the driver, it serves as the source of map outputs recorded from ShuffleMapTasks.
      * On the executors, it simply serves as a cache, in which a miss triggers a fetch from the
      * driver's corresponding HashMap.
      *
      * Note: because mapStatuses is accessed concurrently, subclasses should make sure it's a
      * thread-safe map.
      */
    protected val mapStatuses: Map[Int, Array[MapStatus]]

    /**
      * Incremented every time a fetch fails so that client nodes know to clear
      * their cache of map output locations if this happens.
      */
    protected var epoch: Long = 0
    protected val epochLock = new AnyRef

    /** Remembers which map output locations are currently being fetched on an executor. */
        /*
        * 保存哪个map output输出位置是当前正在从executor拉取的
        * */
    private val fetching = new HashSet[Int]

    /**
      * Send a message to the trackerActor and get its result within a default timeout, or
      * throw a SparkException if this fails.
      * 向trackerActor发送消息获得result，下面两个函数，第二个调用第一个
      */
    protected def askTracker(message: Any): Any = {
        try {
            AkkaUtils.askWithReply(message, trackerActor, retryAttempts, retryIntervalMs, timeout)
        } catch {
            case e: Exception =>
                logError("Error communicating with MapOutputTracker", e)
                throw new SparkException("Error communicating with MapOutputTracker", e)
        }
    }

    /** Send a one-way message to the trackerActor, to which we expect it to reply with true. */
    protected def sendTracker(message: Any) {
        val response = askTracker(message)
        if (response != true) {
            throw new SparkException(
                "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
        }
    }

    /**
      * Called from executors to get the server URIs and output sizes of the map outputs of
      * a given shuffle.
      * 被从executors调用去获取server URLS 和shuffle的输出大小
      */
    /*
    *
    * shuffleId和reduceId到底是什么？？？？？？
    * shuffleId是指时间维度上，当前这个shuffle操作，比如一个action中会多次使用reduceByKey，多次shuffle，
    * 这个shuffleId指的就是第几个reduceByKey
    *
    *
    * reduceId就是bucketId，限制每个MapStatus中获取当前ResultTask需要的每个ShuffleMapTask输出的文件信息（参考HashShuffle）
    *
    *
    * 方法getServerStatuses（）传入了shuffleId和reduceId，shuffleId代表了当前这个stage的上一个stage，
    * 我们知道shuffle是分为两个stage的，shuffle write是发生在上一个stage中，shuffle read是发生在当前的stage中的，
    * 也就是通过shuffleId可用限制到上一个stage的所有shuffleMapTask的输出的MapStatus，
    * 接着通过reduceid，也就是bucketId，来限制从每个MapStatus中，获取当前这个ResultTask需要获取的每个ShuffleMapTask的输出文件的信息，
    * 这个getServerStatuses（）方法一定是走远程网络通信的，因为要联系Driver上的DAGScheduler的MapOutputTrackerMaster
    * */
    def getServerStatuses(shuffleId: Int, reduceId: Int): Array[(BlockManagerId, Long)] = {
        /*
        * 试图从缓存mapstatused中获取结果
        * */
        val statuses = mapStatuses.get(shuffleId).orNull
        /*
        * 如果MapStatuses没有shuffleId的数据，则会像driver请求
        * */
        if (statuses == null) {
            logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
            var fetchedStatuses: Array[MapStatus] = null
            /*
            * 同步锁，保持数据一致性，fetching是当前正在拉取的输出位置map output
            * */
            fetching.synchronized {
                // Someone else is fetching it; wait for them to be done
                while (fetching.contains(shuffleId)) {
                    try {
                        fetching.wait()
                    } catch {
                        case e: InterruptedException =>
                    }
                }

                // Either while we waited the fetch happened successfully, or
                // someone fetched it in between the get and the fetching.synchronized.
                fetchedStatuses = mapStatuses.get(shuffleId).orNull
                if (fetchedStatuses == null) {
                    // We have to do the fetch, get others to wait for us.
                    fetching += shuffleId
                }
            }

            if (fetchedStatuses == null) {
                // We won the race to fetch the output locs; do so
                logInfo("Doing the fetch; tracker actor = " + trackerActor)
                // This try-finally prevents hangs due to timeouts:
                try {
                    val fetchedBytes =
                        askTracker(GetMapOutputStatuses(shuffleId)).asInstanceOf[Array[Byte]]
                    fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
                    logInfo("Got the output locations")
                    /*
                    *
                    * 将远程拉取的结果，添加到mapStatuses
                    * */
                    mapStatuses.put(shuffleId, fetchedStatuses)
                } finally {
                    fetching.synchronized {
                        fetching -= shuffleId
                        fetching.notifyAll()
                    }
                }
            }
            if (fetchedStatuses != null) {
                fetchedStatuses.synchronized {
                    return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, fetchedStatuses)
                }
            } else {
                logError("Missing all output locations for shuffle " + shuffleId)
                throw new MetadataFetchFailedException(
                    shuffleId, reduceId, "Missing all output locations for shuffle " + shuffleId)
            }
        } else {
            statuses.synchronized {
                return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, statuses)
            }
        }
    }

    /** Called to get current epoch number. */
    def getEpoch: Long = {
        epochLock.synchronized {
            return epoch
        }
    }

    /**
      * Called from executors to update the epoch number, potentially clearing old outputs
      * because of a fetch failure. Each executor task calls this with the latest epoch
      * number on the driver at the time it was created.
      */
    /*
    *
    * 被从executors调用去更新epoch次数，然后清除matStatuses缓存
    * */
    def updateEpoch(newEpoch: Long) {
        epochLock.synchronized {
            if (newEpoch > epoch) {
                logInfo("Updating epoch to " + newEpoch + " and clearing cache")
                epoch = newEpoch
                mapStatuses.clear()
            }
        }
    }

    /** Unregister shuffle data. */
    def unregisterShuffle(shuffleId: Int) {
        mapStatuses.remove(shuffleId)
    }

    /** Stop the tracker. */
    def stop() {}
}

/**
  * MapOutputTracker for the driver. This uses TimeStampedHashMap to keep track of map
  * output information, which allows old output information based on a TTL.
  *
  *
  * 对于driver的MapOutputTracker，
  * 使用了TimeStampedHashMap去保存shuffle的输出信息
  * 允许保存咋TTL范围内的旧的输出信息
  */
//////////////////////////////////////////////////很重要的观念//////////////////////////////////////////////////
////////////////////    MapOutputTrackerMaster是真正保存相关shuffle信息的地方，     ////////////////////////////
////////////////////    而之所以有MapOutputTrackerMasterActor这些actor，            ////////////////////////////
////////////////////    只是为了通信，发送或接受保存的信息                          ////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
private[spark] class MapOutputTrackerMaster(conf: SparkConf)
        extends MapOutputTracker(conf) {

    /** Cache a serialized version of the output statuses for each shuffle to send them out faster */
    private var cacheEpoch = epoch

    /**
      * Timestamp based HashMap for storing mapStatuses and cached serialized statuses in the driver,
      * so that statuses are dropped only by explicit de-registering or by TTL-based cleaning (if set).
      * Other than these two scenarios, nothing should be dropped from this HashMap.
      *
      * 旧的mapstatus只有超过TTL才会被删除
      */
        //  为什么需要两个mapStatus、CacheSerializedStatuses变量呢？？
        // 因为（1）mapStatus保存的是shuffle中map的位置信息，（2）而CacheSerializedStatuses保存的是map后真正的序列化值，后半句存疑，需要验证
    protected val mapStatuses = new TimeStampedHashMap[Int, Array[MapStatus]]()
    private val cachedSerializedStatuses = new TimeStampedHashMap[Int, Array[Byte]]()

    // For cleaning up TimeStampedHashMaps
    private val metadataCleaner =
        new MetadataCleaner(MetadataCleanerType.MAP_OUTPUT_TRACKER, this.cleanup, conf)

    def registerShuffle(shuffleId: Int, numMaps: Int) {
        if (mapStatuses.put(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
            throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
        }
    }

    def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
        val array = mapStatuses(shuffleId)
        array.synchronized {
            array(mapId) = status
        }
    }

    /** Register multiple map output information for the given shuffle */
    def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
        mapStatuses.put(shuffleId, Array[MapStatus]() ++ statuses)
        if (changeEpoch) {
            incrementEpoch()
        }
    }

    /** Unregister map output information of the given shuffle, mapper and block manager */
    def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
        val arrayOpt = mapStatuses.get(shuffleId)
        if (arrayOpt.isDefined && arrayOpt.get != null) {
            val array = arrayOpt.get
            array.synchronized {
                if (array(mapId) != null && array(mapId).location == bmAddress) {
                    array(mapId) = null
                }
            }
            incrementEpoch()
        } else {
            throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
        }
    }

    /** Unregister shuffle data */
    override def unregisterShuffle(shuffleId: Int) {
        mapStatuses.remove(shuffleId)
        cachedSerializedStatuses.remove(shuffleId)
    }

    /** Check if the given shuffle is being tracked */
    def containsShuffle(shuffleId: Int): Boolean = {
        cachedSerializedStatuses.contains(shuffleId) || mapStatuses.contains(shuffleId)
    }

    def incrementEpoch() {
        epochLock.synchronized {
            epoch += 1
            logDebug("Increasing epoch to " + epoch)
        }
    }

    //  该函数逻辑：1、如果有现成的序列化的mapStatus，即cachedSerializedStatuses有，则直接拿出来返回；
    //  2、如果没现成的序列化的mapStatus，则从mapStatuses变量里拿出来后，序列化后返回。
    def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
        var statuses: Array[MapStatus] = null
        var epochGotten: Long = -1
        epochLock.synchronized {
            if (epoch > cacheEpoch) {
                cachedSerializedStatuses.clear()
                cacheEpoch = epoch
            }
            //  判断MapOutputTrackerMaster里是否缓存了shuffle信息，如果缓存了，直接拿，然后return，函数结束；
            //  如果没缓存，从MapStatus里重新pull拉数据，然后赋值给statues变量，然后接着执行下面的操作val bytes = MapOutputTracker.serializeMapStatuses(statuses)
            cachedSerializedStatuses.get(shuffleId) match {
                case Some(bytes) =>
                    return bytes
                case None =>
                    statuses = mapStatuses.getOrElse(shuffleId, Array[MapStatus]())
                    epochGotten = epoch
            }
        }
        // If we got here, we failed to find the serialized locations in the cache, so we pulled
        // out a snapshot of the locations as "statuses"; let's serialize and return that
        //  序列化并压缩shuffle的output statuses，即mapStatus
        val bytes = MapOutputTracker.serializeMapStatuses(statuses)
        logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
        // Add them into the table only if the epoch hasn't changed while we were working
        //  重新拉了并序列化mapStatus后，将结果缓存在cachedSerializedStatuses中，下次就可以用了
        epochLock.synchronized {
            if (epoch == epochGotten) {
                cachedSerializedStatuses(shuffleId) = bytes
            }
        }
        bytes
    }

    override def stop() {
        sendTracker(StopMapOutputTracker)
        mapStatuses.clear()
        trackerActor = null
        metadataCleaner.cancel()
        cachedSerializedStatuses.clear()
    }

    private def cleanup(cleanupTime: Long) {
        mapStatuses.clearOldValues(cleanupTime)
        cachedSerializedStatuses.clearOldValues(cleanupTime)
    }
}

/**
  * MapOutputTracker for the executors, which fetches map output information from the driver's
  * MapOutputTrackerMaster.
  *
  * executors端的MapOutputTracker，从driver的MapOutputTrackerMaster拉取map的output信息
  */
/*
*
* 总结就是，
* 1、MapOutputTrackerMaster保存map的output信息（not output内容），
* 2、MapOutputTrackerWorker是从MapOutputTrackerMaster拉取信息
* */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {
    protected val mapStatuses: Map[Int, Array[MapStatus]] =
        new ConcurrentHashMap[Int, Array[MapStatus]]
}

/*
*
* MapOutputTracker类的伴生对象
* */
private[spark] object MapOutputTracker extends Logging {

    // Serialize an array of map output locations into an efficient byte format so that we can send
    // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
    // generally be pretty compressible because many map outputs will be on the same hostname.
    /*
    *
    * 把map的output locations序列化成bytes格式，GZIP压缩并发送给reduce tasks，
    * 由于map output很多都在同一个hostname上，所以压缩率很可观
    * */
    //  序列化并压缩mapoutput信息MapStatus
    def serializeMapStatuses(statuses: Array[MapStatus]): Array[Byte] = {
        val out = new ByteArrayOutputStream
        val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
        // Since statuses can be modified in parallel, sync on it
        statuses.synchronized {
            objOut.writeObject(statuses)
        }
        objOut.close()
        out.toByteArray
    }

    // Opposite of serializeMapStatuses.
    def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {
        val objIn = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))
        objIn.readObject().asInstanceOf[Array[MapStatus]]
    }

    // Convert an array of MapStatuses to locations and sizes for a given reduce ID. If
    // any of the statuses is null (indicating a missing location due to a failed mapper),
    // throw a FetchFailedException.
    /*
    *
    * 把MapStatuses数组转换成给定reduceId的位置和大小
    * */
    private def convertMapStatuses(
                                          shuffleId: Int,
                                          reduceId: Int,
                                          statuses: Array[MapStatus]): Array[(BlockManagerId, Long)] = {
        assert(statuses != null)
        statuses.map {
            status =>
                if (status == null) {
                    logError("Missing an output location for shuffle " + shuffleId)
                    throw new MetadataFetchFailedException(
                        shuffleId, reduceId, "Missing an output location for shuffle " + shuffleId)
                } else {
                    (status.location, status.getSizeForBlock(reduceId))
                }
        }
    }
}
