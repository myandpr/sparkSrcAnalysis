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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
  * An Schedulable entity that represent collection of Pools or TaskSetManagers
  */
/*
*
* TaskSetManager一定是任务调度树里的叶子, 而Pool一定是中间节点. 作为叶子就有一些好玩的特性.
* 理解一点就是TaskSet之间的调度其实是在Pool这个结构里玩的, 而TaskSetManager负责的是针对仅仅一个TaskSet的调度
* 多叉树的非叶子节点
*
* */
private[spark] class Pool(
                                 val poolName: String,
                                 val schedulingMode: SchedulingMode,
                                 initMinShare: Int,
                                 initWeight: Int)
        extends Schedulable
                with Logging {
    //为什么不直接继承Schedulable中的schedulableQueue变量，而要重新定义
    //由于Schedulable只有Pool和TaskSetManager两个实现类，所以SchedulableQueue是一个可以嵌套的层次结构
    //从这里我们看出，所谓的TaskSetManager和Pool本质是一个东西，平行的
    //Schedulable包含schedulableQueue，类似于多叉树的结构，schedulableQueue套着schedulableQueue
    // 这个两个结构联合起来管理这个Pool里所有的child node
    // 可以是pool或者tasksetmanager
    val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
    //  调度名称与Schedulable的对应关系
    val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
    //用于公平调度算法的参考值
    var weight = initWeight
    var minShare = initMinShare
    //  当前正在运行的任务数量
    /* 前面我们看到过, 当这个Pool下面的叶子节点里有task在运行这里就会+1, 它反映的是这个Pool管理的所有的TaskSet共有多少个task在运行
    * */
    var runningTasks = 0
    //  进行调度的优先级
    var priority = 0

    // A pool's stage id is used to break the tie in scheduling.
    //  调度池或TaskSetManager所属的Stage的身份标识
    var stageId = -1
    var name = poolName
    //  当前Pool的父Pool
    var parent: Pool = null

    //  比较叶子节点TaskSetManager的调度顺序算法，不是比的中间节点Pool的调度顺序。
    //  FIFO是先比较priority，再比较stageId
    //  FAIR就比较复杂，是根据minShare runningTasks weight来决定哪个TaskSet先运行.
    var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
        schedulingMode match {
            case SchedulingMode.FAIR =>
                new FairSchedulingAlgorithm()
            case SchedulingMode.FIFO =>
                new FIFOSchedulingAlgorithm()
        }
    }

    //  用于将Schedulable添加到schedulableQueue和schedulableNameToSchedulable中，并将Schedulable的父亲设置为当前Pool
    override def addSchedulable(schedulable: Schedulable) {
        require(schedulable != null)
        schedulableQueue.add(schedulable)
        schedulableNameToSchedulable.put(schedulable.name, schedulable)
        schedulable.parent = this
    }

    //  移除
    override def removeSchedulable(schedulable: Schedulable) {
        schedulableQueue.remove(schedulable)
        schedulableNameToSchedulable.remove(schedulable.name)
    }

    //  用于根据指定名称查找Schedulable
    override def getSchedulableByName(schedulableName: String): Schedulable = {
        if (schedulableNameToSchedulable.containsKey(schedulableName)) {
            return schedulableNameToSchedulable.get(schedulableName)
        }
        //  如果当前Pool中没有名称为SchedulableName的Schedulable，则从“子Schedulable”中查找，因为Pool是一个嵌套的多层次结构
        //  所谓的“子Schedulable”就是当前的schedulable队列
        for (schedulable <- schedulableQueue) {
            val sched = schedulable.getSchedulableByName(schedulableName)
            if (sched != null) {
                return sched
            }
        }
        null
    }

    //  用于当某个Executor丢失后，调用当前Pool的schedulableQueue中各个Schedulable（可能为子调度池，也可能为TaskSetManager）的executorLost方法。
    //  TaskSetManager的executorLost将该Executor上正在运行的Task作为失败处理，内部通过TaskScheduler重新提交这些任务。
    override def executorLost(executorId: String, host: String) {
        schedulableQueue.foreach(_.executorLost(executorId, host))
    }

    //  检查当前Pool中是否有需要推断执行的任务，通过递归的调用Pool的schedulableQueue的每个Schedulable的checkSpeculatableTasks方法遍历
    override def checkSpeculatableTasks(): Boolean = {
        var shouldRevive = false
        for (schedulable <- schedulableQueue) {
            shouldRevive |= schedulable.checkSpeculatableTasks()
        }
        shouldRevive
    }

    //  对当前Pool中的所有TaskSetManager按照调度算法taskSetSchedulingAlgorithm进行排序，并返回叶子节点有序的TaskSetManager。
    //  getSortedTaskSetQueue实际是通过迭代调用schedulableQueue中的各个Schedulable的getSortedTaskSetQueue方法实现。
    //  这个方法实现了对TaskSet的排序, 决定了哪个TaskSet先运行, 哪个后运行
    override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
        var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
        //  第一层排序：当前schedulableQueue中所有Schedulable排序
        val sortedSchedulableQueue =
            schedulableQueue.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
        //  第二层schedulableQueue排序（如此递归）
        for (schedulable <- sortedSchedulableQueue) {
            //  都是把最终的叶子节点TaskSetManager排序了，最小单位还是TaskSetManager，而不是TaskSet中的一个个task
            sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
        }
        sortedTaskSetQueue
    }

    //  用于增加当前Pool及其父Pool中记录的当前正在运行的任务数量
    def increaseRunningTasks(taskNum: Int) {
        runningTasks += taskNum
        if (parent != null) {
            parent.increaseRunningTasks(taskNum)
        }
    }

    def decreaseRunningTasks(taskNum: Int) {
        runningTasks -= taskNum
        if (parent != null) {
            parent.decreaseRunningTasks(taskNum)
        }
    }
}
