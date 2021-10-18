/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

object ErrorHandler {
  @transient
  private[this] val LOG = Logger.getLogger(this.getClass)

  /**
    * clean all the failed data for error path before reload.
    *
    * @param path path to clean
    */
  def clear(path: String): Unit = {
    try {
      val fileSystem  = FileSystem.get(new Configuration())
      val filesStatus = fileSystem.listStatus(new Path(path))
      for (file <- filesStatus) {
        if (!file.getPath.getName.startsWith("reload.") ||
            file.getPath.getName.endsWith(".lastReload")) {
          fileSystem.delete(file.getPath, true)
        }
      }
    } catch {
      case e: Throwable => {
        LOG.error(s"$path cannot be clean, but this error does not affect the import result, " +
                    s"you can only focus on the reload files.",
                  e)
      }
    }
  }

  /**
    * save the failed execute statement.
    *
    * @param buffer buffer saved failed ngql
    * @param path path to write these buffer ngql
    */
  def save(buffer: ArrayBuffer[String], path: String): Unit = {
    LOG.info(s"create reload path $path")
    val fileSystem  = FileSystem.get(new Configuration())
    val targetPath  = new Path(path)
    val errors      = if (fileSystem.exists(targetPath)) {
      // For kafka, the error ngql need to append to a same file instead of overwrite
      fileSystem.append(targetPath)
    } else {
      fileSystem.create(targetPath)
    }

    try {
      for (error <- buffer) {
        errors.writeBytes(error)
        errors.writeBytes("\n")
      }
    } finally {
      errors.close()
    }
  }

  /**
    * rename the stale reload file
    *
    * @param path
    */
  def rename(path: String): Unit = {
    try {
      val fileSystem  = FileSystem.get(new Configuration())
      val filesStatus = fileSystem.listStatus(new Path(path))
      for (file <- filesStatus) {
        val reloadFile = file.getPath.getName
        if (reloadFile.startsWith("reload.")) {
          if(!fileSystem.rename(file.getPath, new Path(reloadFile + ".lastReload"))) {
            LOG.warn(s"Can not rename $reloadFile. The new reload data will append after stale reload data")
          }
        }
      }
    } catch {
      case e: Throwable => {
        LOG.error(s"Stale reload file in $path cannot be rename ", e)
      }
    }
  }

  /**
    * check if path exists
    *
    * @param path error path
    *@return true if path exists
    */
  def existError(path: String): Boolean = {
    val fileSystem = FileSystem.get(new Configuration())
    fileSystem.exists(new Path(path))
  }
}
