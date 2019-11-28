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

package org.apache.spark.shuffle

import org.apache.spark.{TaskContext, ShuffleDependency}

/**
  * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
  * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
  * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
  *
  * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
  * boolean isDriver as parameters.
  */

//  一个ShuffleManager在driver端和每个executor端的SparkEnv中创建（主要取决于SparkEnv的createSparkEnv函数的参数isDriver是否为true），
// driver注册了shuffle，然后executors可以向driver端去读写数据
private[spark] trait ShuffleManager {
    /**
      * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
      */
    def registerShuffle[K, V, C](
                                        shuffleId: Int,
                                        numMaps: Int,
                                        dependency: ShuffleDependency[K, V, C]): ShuffleHandle

    /** Get a writer for a given partition. Called on executors by map tasks. */
    //  获取某个partition（partition就是mapId，这两个参数是一致的）的writer，这个函数被executors上的map task调用
    //  这个writer可能是用来写入的句柄，这里值得商榷，需要研究一下才能写一个确定的注释。
    //  getWriter和getReader函数，就是map task和reduce task分别获取的对应的writer和reader，去写入和读取，是符合shuffle的write阶段和read阶段的
    def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

    /**
      * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
      * Called on executors by reduce tasks.
      */
    //  被executor上的reduce task调用，去获取一个reader句柄，这个句柄是针对一个reduce端的partition范围的
    def getReader[K, C](
                               handle: ShuffleHandle,
                               startPartition: Int,
                               endPartition: Int,
                               context: TaskContext): ShuffleReader[K, C]

    /**
      * Remove a shuffle's metadata from the ShuffleManager.
      *
      * @return true if the metadata removed successfully, otherwise false.
      */
    //  从ShuffleManager中移除shuffle的metadata
    def unregisterShuffle(shuffleId: Int): Boolean

    def shuffleBlockManager: ShuffleBlockManager

    /** Shut down this ShuffleManager. */
    def stop(): Unit
}
