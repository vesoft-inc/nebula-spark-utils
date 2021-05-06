/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange

import java.io.IOException

import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

object ErrorHandler {
  private[this] lazy val LOG = Logger.getLogger(this.getClass)
  def save(buffer: ArrayBuffer[String], path: String): Unit = {
    val fileSystem = FileSystem.get(new Configuration())
    val errors     = fileSystem.create(new Path(path))
    try {
      for (error <- buffer) {
        errors.writeBytes(error)
        errors.writeBytes("\n")
      }
    } finally {
      errors.close()
    }
  }

  def remove(path:String)={
    val fileSystem =FileSystem.get(new Configuration())
    var result=false
    try {
      result=fileSystem.delete(new Path(path), true)
      LOG.info(s"Remove File Path $path succeed")
    }catch {
      case e:IOException=>LOG.error(s"Remove File Path $path failed")
      case e:Exception=>LOG.error(ExceptionUtils.getFullStackTrace(e))
    }
    result
  }

  def fileExists(path: String): Boolean = {
    val fileSystem = FileSystem.get(new Configuration())
    val existed = fileSystem.exists(new Path(path))
    if(!existed){
      false
    }else{
      fileSystem.listFiles(new Path(path),true).hasNext
    }
  }

  def rename(src:String,dst:String)={
    val fileSystem =FileSystem.get(new Configuration())
    val result = fileSystem.rename(new Path(src),new Path(dst))
    LOG.info(s"Rename File Path From $src To $dst")
    result
  }

  def moveOldFilesIfExist(path:String): Unit ={
    val origin =path
    if(fileExists(origin)){
      var index=1
      var continue=true
      while(continue){
        if(fileExists(s"${origin}_old_$index")){
          index+=1
        }else{
          rename(origin,s"${origin}_old_$index")
          continue=false
        }
      }
    }
  }

}
