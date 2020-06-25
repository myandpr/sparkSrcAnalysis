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

package org.apache.spark.storage

import java.util.UUID
import java.io.{IOException, File}

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.Utils

/**
  * Creates and maintains the logical mapping between logical blocks and physical on-disk
  * locations. By default, one block is mapped to one file with a name given by its BlockId.
  * However, it is also possible to have a block map to only a segment of a file, by calling
  * mapBlockToFileSegment().
  *
  * Block files are hashed among the directories listed in spark.local.dir (or in
  * SPARK_LOCAL_DIRS, if it's set).
  */
//  维持一个逻辑block和屋里磁盘位置之间的逻辑映射。默认，一个block被映射到一个名字是BlockId的文件file。
//  然而，也可能一个block仅仅映射一个文件的段segment，通过调用mapBlockToFileSegment()函数
//  Block files被hash映射到spark.local.dir的一些列目录


//  该DiskBlockManager最重要的部分就是：1、localDirs、subDirs；2、getFile()
private[spark] class DiskBlockManager(blockManager: BlockManager, conf: SparkConf)
        extends Logging {

    private[spark]
    val subDirsPerLocalDir = blockManager.conf.getInt("spark.diskStore.subDirectories", 64)

    /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
     * directory, create multiple subdirectories that we will hash files into, in order to avoid
     * having really large inodes at the top level. */
    //  给spark.local.dir的每个path都创建一个本地目录，在这个目录下，创建多个子目录用来hash各个file进去
    //  spark.local.dir用于存储mapper输出文件和缓存到磁盘的RDD数据，可以用逗号分隔指定多个目录；最好将这个属性设定为访问速度快的本地磁盘SSD什么的更好
    private[spark] val localDirs: Array[File] = createLocalDirs(conf)
    if (localDirs.isEmpty) {
        logError("Failed to create any local dir.")
        System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
    }
    //  二维数组，多个主目录，每个目录下有64个子目录
    private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

    private val shutdownHook = addShutdownHook()

    /** Looks up a file by hashing it into one of our local subdirectories. */
    // This method should be kept in sync with
    // org.apache.spark.network.shuffle.StandaloneShuffleBlockManager#getFile().
    //  BlockId.name就是filename
    //  通过文件名，查找到文件File对象
    //  先hash，然后对主目录和子目录求模，然后加上filename，组成了File对象返回
    //  也就印证了片头指出的“DiskBlockManager主要是映射了逻辑的block和物理的file之间的关系”，即block----File
    def getFile(filename: String): File = {
        // Figure out which local directory it hashes to, and which subdirectory in that
        val hash = Utils.nonNegativeHash(filename)
        //  主目录hash索引
        val dirId = hash % localDirs.length
        //  子目录hash索引
        val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

        // Create the subdirectory if it doesn't already exist
        var subDir = subDirs(dirId)(subDirId)
        if (subDir == null) {
            subDir = subDirs(dirId).synchronized {
                val old = subDirs(dirId)(subDirId)
                if (old != null) {
                    old
                } else {
                    //  创建这个子目录，localDirs(dirId)是父目录，"%02x"是子目录
                    val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
                    if (!newDir.exists() && !newDir.mkdir()) {
                        throw new IOException(s"Failed to create local dir in $newDir.")
                    }
                    //  把该目录保存在subDirs结构体中
                    subDirs(dirId)(subDirId) = newDir
                    newDir
                }
            }
        }

        new File(subDir, filename)
    }

    def getFile(blockId: BlockId): File = getFile(blockId.name)

    /** Check if disk block manager has a block. */
    def containsBlock(blockId: BlockId): Boolean = {
        //  在操作系统中，查找是否有该文件
        getFile(blockId.name).exists()
    }

    /** List all the files currently stored on disk by the disk manager. */
    def getAllFiles(): Seq[File] = {
        // Get all the files inside the array of array of directories
        //  获取所有子文件目录，然后罗列出目录中的所有文件
        subDirs.flatten.filter(_ != null).flatMap { dir =>
            val files = dir.listFiles()
            if (files != null) files else Seq.empty
        }
    }

    /** List all the blocks currently stored on disk by the disk manager. */
    def getAllBlocks(): Seq[BlockId] = {
        getAllFiles().map(f => BlockId(f.getName))
    }

    /** Produces a unique block id and File suitable for storing local intermediate results. */
    def createTempLocalBlock(): (TempLocalBlockId, File) = {
        var blockId = new TempLocalBlockId(UUID.randomUUID())
        while (getFile(blockId).exists()) {
            blockId = new TempLocalBlockId(UUID.randomUUID())
        }
        (blockId, getFile(blockId))
    }

    /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
    def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
        var blockId = new TempShuffleBlockId(UUID.randomUUID())
        //  冲突的话，就再创建一个随机blockId
        while (getFile(blockId).exists()) {
            blockId = new TempShuffleBlockId(UUID.randomUUID())
        }
        (blockId, getFile(blockId))
    }

    private def createLocalDirs(conf: SparkConf): Array[File] = {
        Utils.getOrCreateLocalRootDirs(conf).flatMap { rootDir =>
            try {
                val localDir = Utils.createDirectory(rootDir, "blockmgr")
                logInfo(s"Created local directory at $localDir")
                Some(localDir)
            } catch {
                case e: IOException =>
                    logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
                    None
            }
        }
    }

    private def addShutdownHook(): Thread = {
        val shutdownHook = new Thread("delete Spark local dirs") {
            override def run(): Unit = Utils.logUncaughtExceptions {
                logDebug("Shutdown hook called")
                DiskBlockManager.this.doStop()
            }
        }
        Runtime.getRuntime.addShutdownHook(shutdownHook)
        shutdownHook
    }

    /** Cleanup local dirs and stop shuffle sender. */
    private[spark] def stop() {
        // Remove the shutdown hook.  It causes memory leaks if we leave it around.
        try {
            Runtime.getRuntime.removeShutdownHook(shutdownHook)
        } catch {
            case e: IllegalStateException => None
        }
        doStop()
    }

    private def doStop(): Unit = {
        // Only perform cleanup if an external service is not serving our shuffle files.
        if (!blockManager.externalShuffleServiceEnabled || blockManager.blockManagerId.isDriver) {
            localDirs.foreach { localDir =>
                if (localDir.isDirectory() && localDir.exists()) {
                    try {
                        if (!Utils.hasRootAsShutdownDeleteDir(localDir)) Utils.deleteRecursively(localDir)
                    } catch {
                        case e: Exception =>
                            logError(s"Exception while deleting local spark dir: $localDir", e)
                    }
                }
            }
        }
    }
}
