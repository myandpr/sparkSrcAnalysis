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

import java.util.Properties

/**
  * A set of tasks submitted together to the low-level TaskScheduler, usually representing
  * missing partitions of a particular stage.
  */
/*
*
* TaskSet是Task的集合，这个结构维护了这个stage中的所有task
* 一个stage过程是对所有partition执行transform操作，直到遇到一个shuffle，本地partition数据不足以支撑计算，需要进行节点通信。
* stage中的task数量和partition的数量是1:1的，但由于整个集群资源有限，所以stage里的所有task并不是同时跑的，而是按照资源配置尽可能同时跑，除非内存足够，否则有先后顺序，所以存在参数attempt
* TaskSet有顺序，由TaskSetManager管理，FIFO。TaskSet包含的是需要被执行的task。
* */
private[spark] class TaskSet(
                                    val tasks: Array[Task[_]],
                                    val stageId: Int,
                                    val attempt: Int,
                                    val priority: Int,
                                    val properties: Properties) {
    val id: String = stageId + "." + attempt

    override def toString: String = "TaskSet " + id
}
