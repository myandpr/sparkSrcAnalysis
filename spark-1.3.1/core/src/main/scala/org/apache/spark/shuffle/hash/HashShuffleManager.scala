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
import org.apache.spark.shuffle._

/**
  * A ShuffleManager using hashing, that creates one output file per reduce partition on each
  * mapper (possibly reusing these across waves of tasks).
  */
//  使用hash的ShuffleManager： 每个mapper为每个reduce创建了一个输出文件output file。
private[spark] class HashShuffleManager(conf: SparkConf) extends ShuffleManager {

    //  该类没有梳理，需要梳理一下
    private val fileShuffleBlockManager = new FileShuffleBlockManager(conf)

    /* Register a shuffle with the manager and obtain a handle for it to pass to tasks. */
    //  还是要研究参数dependency到底是什么！！！！！！！！！！！！！！！！！！！！
    override def registerShuffle[K, V, C](
                                                 shuffleId: Int,
                                                 numMaps: Int,
                                                 dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
        new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }

    /**
      * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
      * Called on executors by reduce tasks.
      */
    /////////////////////////////////////////////////////////////////////////////////////////////////
    //////   注意：HashShuffleManager和SortShuffleManager的getReader()都是HashShuffleReader    //////
    /////////////////////////////////////////////////////////////////////////////////////////////////
    override def getReader[K, C](
                                        handle: ShuffleHandle,
                                        startPartition: Int,
                                        endPartition: Int,
                                        context: TaskContext): ShuffleReader[K, C] = {
        new HashShuffleReader(
            handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
    }

    /** Get a writer for a given partition. Called on executors by map tasks. */
    //  根据给定的map task的分区，获得一个writer，这个函数是executor上的map tasks调用的
    //  是不是意味着每个map task都要有一个writer？？？？？
    //  是的，Ctrl + 鼠标左键，就可以发现ShuffleMapTask.runTask()函数中从ShuffleManager中调用了manager.getWriter，获取了writer句柄
    override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
    : ShuffleWriter[K, V] = {
        new HashShuffleWriter(
            shuffleBlockManager, handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)
    }

    /** Remove a shuffle's metadata from the ShuffleManager. */
    override def unregisterShuffle(shuffleId: Int): Boolean = {
        shuffleBlockManager.removeShuffle(shuffleId)
    }

    override def shuffleBlockManager: FileShuffleBlockManager = {
        fileShuffleBlockManager
    }

    /** Shut down this ShuffleManager. */
    override def stop(): Unit = {
        shuffleBlockManager.stop()
    }
}
