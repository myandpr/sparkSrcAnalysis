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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
  * An interface for schedulable entities.
  * there are two type of Schedulable entities(Pools and TaskSetManagers)
  */
private[spark] trait Schedulable {
    var parent: Pool

    // child queues
    //由于Schedulable只有Pool和TaskSetManager两个实现类，所以SchedulableQueue是一个可以嵌套的层次结构
    //从这里我们看出，所谓的TaskSetManager和Pool本质是一个东西，平行的
    def schedulableQueue: ConcurrentLinkedQueue[Schedulable]

    def schedulingMode: SchedulingMode
    //用于公平调度算法的权重
    def weight: Int

    def minShare: Int

    def runningTasks: Int

    def priority: Int

    def stageId: Int

    def name: String

    def addSchedulable(schedulable: Schedulable): Unit

    def removeSchedulable(schedulable: Schedulable): Unit

    def getSchedulableByName(name: String): Schedulable

    def executorLost(executorId: String, host: String): Unit

    def checkSpeculatableTasks(): Boolean

    def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
}
