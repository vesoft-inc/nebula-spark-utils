/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.processor

import java.util.UUID

import com.vesoft.nebula.exchange.{ErrorHandler, GraphProvider}
import com.vesoft.nebula.exchange.config.Configs
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

class ReloadProcessor(data: DataFrame,
                      config: Configs,
                      batchSuccess: LongAccumulator,
                      batchFailure: LongAccumulator)
  extends Processor {
  private[this] lazy val LOG = Logger.getLogger(this.getClass)

  override def process(): Unit = {
    data.foreachPartition(processEachPartition(_))
  }

  private def processEachPartition(iterator: Iterator[Row]): Unit = {
    val graphProvider = new GraphProvider(config.databaseConfig.getGraphAddress)
    val session       = graphProvider.getGraphClient(config.userConfig)
    if (session == null) {
      throw new IllegalArgumentException("connect to graph failed.")
    }
    val switchResult = graphProvider.switchSpace(session,config.databaseConfig.space)
    if(!switchResult){
      throw new RuntimeException("Swtich Space Failed")
    }
    LOG.info(s"Connection to ${config.databaseConfig.metaAddresses}")

    val errorBuffer = ArrayBuffer[String]()

    iterator.foreach(row => {
      val exec   = row.getString(0)
      val result = session.execute(exec)
      if (result == null || !result.isSucceeded) {
        errorBuffer.append(exec)
        batchFailure.add(1)
      } else {
        batchSuccess.add(1)
      }
    })
    if (errorBuffer.nonEmpty) {
      ErrorHandler.save(errorBuffer,
        s"${config.errorConfig.errorPath}/${config.errorConfig.errorPathId}/${config.databaseConfig.space}/reload_tmp/reload.${TaskContext.getPartitionId()}")
      errorBuffer.clear()
    }else{

    }
  }
}

