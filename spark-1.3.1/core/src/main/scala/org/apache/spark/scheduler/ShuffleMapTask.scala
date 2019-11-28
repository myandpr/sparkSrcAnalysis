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

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
  * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
  * specified in the ShuffleDependency).
  *
  * See [[org.apache.spark.scheduler.Task]] for more information.
  *
  * @param stageId    id of the stage this task belongs to
  * @param taskBinary broadcast version of of the RDD and the ShuffleDependency. Once deserialized,
  *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
  * @param partition  partition of the RDD this task is associated with       //  这个task对应的该RDD的partition
  * @param locs       preferred task execution locations for locality scheduling
  */
private[spark] class ShuffleMapTask(
                                           stageId: Int,
                                           taskBinary: Broadcast[Array[Byte]],
                                           partition: Partition,    //  该task对应的partition，一个task对应一个partition
                                           @transient private var locs: Seq[TaskLocation])
        extends Task[MapStatus](stageId, partition.index) with Logging {

    /** A constructor used only in test suites. This does not require passing in an RDD. */
    def this(partitionId: Int) {
        this(0, null, new Partition {
            override def index = 0
        }, null)
    }

    @transient private val preferredLocs: Seq[TaskLocation] = {
        if (locs == null) Nil else locs.toSet.toSeq
    }

    /*
    * runTask核心功能就是安分区写入file
    * val manager = SparkEnv.get.shuffleManager
    * writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
    * writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
    *
    * */
    /*
    *
    * ShuffleMapTask主要关注中间结果，主要是把partition操作等待shuffle到别的机器上的数据管理起来。
    *   这runTask只是对应一个分区，就是改task对应的那个分区
    * */
    override def runTask(context: TaskContext): MapStatus = {
        // Deserialize the RDD using the broadcast variable.
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
            ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

        metrics = Some(context.taskMetrics)
        var writer: ShuffleWriter[Any, Any] = null
        try {
            val manager = SparkEnv.get.shuffleManager
            //  很显然，每个map task（也就是每个partition分区）都会获取一个writer句柄去写入文件
            writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
            //  调用write函数，对一个分区partition的RDD数据进行写入文件操作
            //  返回值Iterator为Scala自带类，参数split通过查看Partition不难看出是一个RDD的一个分区的标识，
            //  也就是说，通过输入参数某个分区的标识就可以获得这个分区的数据集合的迭代器，RDD与实际的某台机器上的数据集合就是这么联系起来的。
            //  RDD的Iterator方法只有这么一个，但是这个方法只能用来遍历某个Partition的数据，不能遍历整个RDD中的全部数据。
            writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
            return writer.stop(success = true).get
        } catch {
            case e: Exception =>
                try {
                    if (writer != null) {
                        writer.stop(success = false)
                    }
                } catch {
                    case e: Exception =>
                        log.debug("Could not stop writer", e)
                }
                throw e
        }
    }

    override def preferredLocations: Seq[TaskLocation] = preferredLocs

    override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
