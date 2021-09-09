/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.fix.nebula

import java.util.concurrent.TimeUnit

import com.google.common.util.concurrent.RateLimiter
import com.vesoft.nebula.connector.NebulaOptions
import org.slf4j.LoggerFactory

import scala.util.control.Breaks

class NebulaWriter(nebulaOptions: NebulaOptions)  {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  private val address: List[(String, Int)] = nebulaOptions.getGraphAddress

  var graphProvider: GraphProvider = _

  {
    if (address != null && address.size > 1) {
      val index = (new util.Random).nextInt(address.size)
      val list = List.apply(address(index))
      graphProvider = new GraphProvider(list)
    } else {
      graphProvider = new GraphProvider(address)
    }
  }

  def prepareSpace(): Unit = {
    graphProvider.switchSpace(nebulaOptions.user, nebulaOptions.passwd, nebulaOptions.spaceName)
  }

  def submit(exec: String): Unit = {
    @transient val rateLimiter = RateLimiter.create(nebulaOptions.rateLimit)
    val loop = new Breaks
    loop.breakable {
      while (true) {
        if (rateLimiter.tryAcquire(nebulaOptions.rateTimeOut, TimeUnit.MILLISECONDS)) {
          val result = graphProvider.submit(exec)
          if (!result.isSucceeded) {
            LOG.error("failed to write for " + exec + ",error:" + result.getErrorMessage)
          } else {
            LOG.info("batch write succeed:")
            LOG.debug("batch write succeed: " + exec)
            loop.break
          }
        } else {
          LOG.error(s"failed to acquire reteLimiter for statement")
        }
      }
    }
  }


}
