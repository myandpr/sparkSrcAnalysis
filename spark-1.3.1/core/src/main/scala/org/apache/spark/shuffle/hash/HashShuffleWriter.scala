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

package org.apache.spark.shuffle.hash

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.storage.BlockObjectWriter
//  ShuffleWriter就一个作用，核心函数就一个：write()写文件，写某个partition中的数据
//  Partition : ShuffleMapTask : ShuffleWriter = 1 : 1 : 1
//  ShuffleManager是一个executor中一个SparkEnv，里面创建一个ShuffleManager
//  所以ShuffleMapTask : ShuffleManager = N : 1，一个executor中的所有task都用一个ShuffleManager
private[spark] class HashShuffleWriter[K, V](
                                                    shuffleBlockManager: FileShuffleBlockManager,
                                                    handle: BaseShuffleHandle[K, V, _],
                                                    mapId: Int,
                                                    context: TaskContext)
        extends ShuffleWriter[K, V] with Logging {

    private val dep = handle.dependency
    //  这个应该是reduce端的分区数量？？？？？？？？
    private val numOutputSplits = dep.partitioner.numPartitions
    private val metrics = context.taskMetrics

    // Are we in the process of stopping? Because map tasks can call stop() with success = true
    // and then call stop() with success = false if they get an exception, we want to make sure
    // we don't try deleting files, etc twice.
    private var stopping = false

    private val writeMetrics = new ShuffleWriteMetrics()
    metrics.shuffleWriteMetrics = Some(writeMetrics)

    private val blockManager = SparkEnv.get.blockManager
    private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
    private val shuffle = shuffleBlockManager.forMapTask(dep.shuffleId, mapId, numOutputSplits, ser,
        writeMetrics)

    /** Write a bunch of records to this task's output */
    //  返回值Iterator为Scala自带类，参数split通过查看Partition不难看出是一个RDD的一个分区的标识，
    //  也就是说，通过输入参数某个分区的标识就可以获得这个分区的数据集合的迭代器，RDD与实际的某台机器上的数据集合就是这么联系起来的。
    //  RDD的Iterator方法只有这么一个，但是这个方法只能用来遍历某个Partition的数据，不能遍历整个RDD中的全部数据。
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////参数records是一个分区的迭代器，只能遍历一个分区内的数据/////////////////////////////////////////////
    ////////////////////////////该write函数是一个task作用于一个partition时候，执行的函数////////////////////////////////////////////
    override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
        /*
        *
        * 错误理解： 返回的应该是分区迭代器，遍历每个分区
        * 正确理解： 上面这一行的理解是错误的！！是分区迭代器，但是是某个分区内的迭代器，遍历的是该分区内的所有数据，而不是所有分区的数据。
        * */
        //  aggregator参数表示map或reduce端是否需要聚合。isDefined表示dep的aggretator参数不为空
        val iter = if (dep.aggregator.isDefined) {
            /*
            * 如果map端要求聚合dep.aggregator.isDefined
            * */
            if (dep.mapSideCombine) {
                //  返回还是一个分区的迭代器Iterator[(K, C)]，只不过是map聚合后的迭代器，遍历该分区
                dep.aggregator.get.combineValuesByKey(records, context)
            } else {    //  如果是reduce端要求聚合
                records
            }
        } else {
            require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
            records
        }

        /*
        *
        *
        * 该task对应的分区的每个elem元素，hash算法获得对应bucketId；
        * 每个map都对应的多个bucketId，bucketId编号都是0、1、2、3、4、5、、、numPartition；
        * */
        for (elem <- iter) {
            //  bucketId的范围是[0, 1, 2, 3, 4, ......, numPartitions - 1]
            val bucketId = dep.partitioner.getPartition(elem._1)
            shuffle.writers(bucketId).write(elem)
        }
    }

    /** Close this writer, passing along whether the map completed */
    override def stop(initiallySuccess: Boolean): Option[MapStatus] = {
        var success = initiallySuccess
        try {
            if (stopping) {
                return None
            }
            stopping = true
            if (success) {
                try {
                    Some(commitWritesAndBuildStatus())
                } catch {
                    case e: Exception =>
                        success = false
                        revertWrites()
                        throw e
                }
            } else {
                revertWrites()
                None
            }
        } finally {
            // Release the writers back to the shuffle block manager.
            if (shuffle != null && shuffle.writers != null) {
                try {
                    shuffle.releaseWriters(success)
                } catch {
                    case e: Exception => logError("Failed to release shuffle writers", e)
                }
            }
        }
    }

    private def commitWritesAndBuildStatus(): MapStatus = {
        // Commit the writes. Get the size of each bucket block (total block size).
        val sizes: Array[Long] = shuffle.writers.map { writer: BlockObjectWriter =>
            writer.commitAndClose()
            writer.fileSegment().length
        }
        MapStatus(blockManager.shuffleServerId, sizes)
    }

    private def revertWrites(): Unit = {
        if (shuffle != null && shuffle.writers != null) {
            for (writer <- shuffle.writers) {
                writer.revertPartialWritesAndClose()
            }
        }
    }
}
