# sparkSrcAnalysis
spark-1.3.1版本源码解析，主要针对core部分
## Cluster Manager通信机制
## 任务调度机制
### 跟一条线，TaskSchedulerImpl.submitTasks(taskSet: TaskSet)提交task运行流程
```
1、TaskSchedulerImpl.submitTasks(taskSet: TaskSet)
TaskSchedulerImpl.submitTasks(taskSet: TaskSet){
    val manager = createTaskSetManager(taskSet, maxTaskFailures)
    schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
    //该backend就是CoarseGrainedSchedulerBackend
    backend.reviveOffers()
}
2、backend.reviveOffers()
CoarseGrainedSchedulerBackend.reviveOffers(){
    driverActor ! ReviveOffers
}
3、CoarseGrainedSchedulerBackend.driverActor ! ReviveOffers
CoarseGrainedSchedulerBackend withSchedulerBackend{
    class DriverActor extends Actor {
        def receiveWithLogging = {
            case RegisterExecutor =>
            case StatusUpdate =>
            case ReviveOffers => makeOffers()
            case KillTask =>
            case StopDerver =>
            case StopExecutors =>
            case RemoveExecutor =>
            
        }
    }
}
4、case ReviveOffers => makeOffers()
def makeOffers() {
            launchTasks(scheduler.resourceOffers(executorDataMap.map { case (id, executorData) =>
                new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
            }.toSeq))
        }
5、def launchTasks(tasks: Seq[Seq[TaskDescription]]) 
def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
    executorData.executorActor ! LaunchTask(new SerializableBuffer(serializedTask))
} 
6、CoarseGrainedExecutorBackend ! LaunchTask(new SerializableBuffer(serializedTask))
private[spark] class CoarseGrainedExecutorBackend
        extends Actor with ActorLogReceive with ExecutorBackend {
            override def receiveWithLogging = {
                case RegisteredExecutor =>
                case RegisterExecutorFailed(message) =>
                case LaunchTask(data) =>
                executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
                    taskDesc.name, taskDesc.serializedTask)
                case KillTask(taskId, _, interruptThread) =>
                case StopExecutor =>

            }

            override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
        driver ! StatusUpdate(executorId, taskId, state, data)
    }
        }
7、case LaunchTask(data) => executor.launchTask()
def launchTask(){
    val tr = new TaskRunner(taskId)
    threadPool.execute(tr)
}
8、new TaskRunner(taskId)
class TaskRunner(taskId) Runnable{
    override def run() {
        val serializedResult = {}
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)
    }

}
6、CoarseGrainedExecutorBackend.statusUpdate()
override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
        driver ! StatusUpdate(executorId, taskId, state, data)
    }
7、该driver还是CoarseGrainedSchedulerBackend的DriverActor

```
下面是这条线上，一些重要类的简化：
```
CoarseGrainedSchedulerBackend withSchedulerBackend{
    class DriverActor extends Actor {
        def receiveWithLogging = {
            case RegisterExecutor =>
            case StatusUpdate =>
            case ReviveOffers => makeOffers()
            case KillTask =>
            case StopDerver =>
            case StopExecutors =>
            case RemoveExecutor =>
            
        }
    }
}
```
```
private[spark] class CoarseGrainedExecutorBackend
        extends Actor with ActorLogReceive with ExecutorBackend {
            override def receiveWithLogging = {
                case RegisteredExecutor =>
                case RegisterExecutorFailed(message) =>
                case LaunchTask(data) =>
                executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,taskDesc.name, taskDesc.serializedTask)
                case KillTask(taskId, _, interruptThread) =>
                case StopExecutor =>

            }
            override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
        driver ! StatusUpdate(executorId, taskId, state, data)
    }
        }
```
```
Executor class{

}
```
### 跟一条线，从DAGScheduler.submitMissingTasks提交开始
```
1、DAGScheduler.submitMissingTasks(stage: Stage, jobId: Int)
private def submitMissingTasks(stage: Stage, jobId: Int) {
    val tasks: Seq[Task[_]] = if (stage.isShuffleMap) {
        new ShuffleMapTask(stage.id, taskBinary, part, locs)
    } else{
        new ResultTask(stage.id, taskBinary, part, locs, id)
    }
    taskScheduler.submitTasks(new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
}
2、TaskSchedulerImpl.submitTask(taskSet: TaskSet)
override def submitTasks(taskSet: TaskSet) {
    val manager = createTaskSetManager(taskSet, maxTaskFailures)
    schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
    backend.reviveOffers()
}
```
接下来就是上一条线了。
### AppClient这条线梳理
### 一些零散知识点
#### defaultParallelism调用
```
SparkContext.parallelize(defaultParallelism) -> TaskSchedulerImpl.defaultParallelism -> CoarseGrainedSchedulerBackend.defaultParallelism()
    override def defaultParallelism(): Int = {
        conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
    }
```
