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

import org.apache.spark.scheduler.MapStatus

/**
  * Obtained inside a map task to write out records to the shuffle system.
  */
//  ShuffleWriter就一个作用，write()写文件，写某个partition中的数据
//  Partition : ShuffleMapTask : ShuffleWriter = 1 : 1 : 1
private[spark] trait ShuffleWriter[K, V] {
    /** Write a bunch of records to this task's output */
    //  返回值Iterator为Scala自带类，参数split通过查看Partition不难看出是一个RDD的一个分区的标识，
    // 也就是说，通过输入参数某个分区的标识就可以获得这个分区的数据集合的迭代器，RDD与实际的某台机器上的数据集合就是这么联系起来的。
    //  RDD的Iterator方法只有这么一个，但是这个方法只能用来遍历某个Partition的数据，不能遍历整个RDD中的全部数据。
    def write(records: Iterator[_ <: Product2[K, V]]): Unit

    /** Close this writer, passing along whether the map completed */
    def stop(success: Boolean): Option[MapStatus]
}
