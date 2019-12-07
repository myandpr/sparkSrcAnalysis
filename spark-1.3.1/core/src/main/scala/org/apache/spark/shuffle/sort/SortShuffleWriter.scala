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

package org.apache.spark.shuffle.sort

import org.apache.spark.{MapOutputTracker, SparkEnv, Logging, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{IndexShuffleBlockManager, ShuffleWriter, BaseShuffleHandle}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
                                                       shuffleBlockManager: IndexShuffleBlockManager,
                                                       handle: BaseShuffleHandle[K, V, C],
                                                       mapId: Int,
                                                       context: TaskContext)
        extends ShuffleWriter[K, V] with Logging {

    private val dep = handle.dependency

    private val blockManager = SparkEnv.get.blockManager

    private var sorter: ExternalSorter[K, V, _] = null

    // Are we in the process of stopping? Because map tasks can call stop() with success = true
    // and then call stop() with success = false if they get an exception, we want to make sure
    // we don't try deleting files, etc twice.
    private var stopping = false

    private var mapStatus: MapStatus = null

    private val writeMetrics = new ShuffleWriteMetrics()
    context.taskMetrics.shuffleWriteMetrics = Some(writeMetrics)

    /** Write a bunch of records to this task's output */
    //  一个map task对应一个partition，将这个partition中的数据写入文件中
    //  该records参数表示该map task所在partition的迭代器，用来遍历该partition内的所有数据
    override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
        //  mapSideCombine：是否在map端进行Combine操作,默认为true，注意是map端的
        if (dep.mapSideCombine) {
            //  这里有个疑惑的地方：aggregator和mapSideCombine的关系
            //  看翻译像是“没有指定具体聚合器的map端合并”，函数require()的意思是“如果!dep.aggregator.isDefined = true，则抛出异常，打印出后面的message”
            require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
            //  建立一个外部排序器，在这个ExeternalSorter中将该partition中的数据进行排序
            sorter = new ExternalSorter[K, V, C](
                dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
            sorter.insertAll(records)
        } else {
            // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
            // care whether the keys get sorted in each partition; that will be done on the reduce side
            // if the operation being run is sortByKey.
            sorter = new ExternalSorter[K, V, V](
                None, Some(dep.partitioner), None, dep.serializer)
            sorter.insertAll(records)
        }

        // Don't bother including the time to open the merged output file in the shuffle write time,
        // because it just opens a single file, so is typically too fast to measure accurately
        // (see SPARK-3570).
        val outputFile = shuffleBlockManager.getDataFile(dep.shuffleId, mapId)
        val blockId = shuffleBlockManager.consolidateId(dep.shuffleId, mapId)
        val partitionLengths = sorter.writePartitionedFile(blockId, context, outputFile)
        shuffleBlockManager.writeIndexFile(dep.shuffleId, mapId, partitionLengths)

        mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    }

    /** Close this writer, passing along whether the map completed */
    override def stop(success: Boolean): Option[MapStatus] = {
        try {
            if (stopping) {
                return None
            }
            stopping = true
            if (success) {
                return Option(mapStatus)
            } else {
                // The map task failed, so delete our output data.
                shuffleBlockManager.removeDataByMap(dep.shuffleId, mapId)
                return None
            }
        } finally {
            // Clean up our sorter, which may have its own intermediate files
            if (sorter != null) {
                sorter.stop()
                sorter = null
            }
        }
    }
}
