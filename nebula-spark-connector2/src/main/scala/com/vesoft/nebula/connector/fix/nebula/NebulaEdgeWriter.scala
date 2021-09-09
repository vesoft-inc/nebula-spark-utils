/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.fix.nebula

import com.vesoft.nebula.connector.NebulaOptions
import com.vesoft.nebula.connector.fix.config.{EdgeConfig, NebulaConfig}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, Encoders}

import scala.collection.mutable.ListBuffer

/**
  * @Description NebulaEdgeWriter is used for 
  * @author huangzhaolai-jk
  * @Date 2021/8/2 - 15:54 
  * @version 1.0.0
  */
class NebulaEdgeWriter(nebulaOptions: NebulaOptions, edgeConfig: EdgeConfig, nebulaConfig: NebulaConfig) extends NebulaBaseWriter {

  private var properties: Map[String, Integer] = Map()

  override def write(rdd: DataFrame): Unit = {
    val srcKeyPolicy = if (StringUtils.isBlank(edgeConfig.getSrcKeyPolicy)) Option.empty else Option(KeyPolicy.withName(edgeConfig.getSrcKeyPolicy))
    val dstKeyPolicy = if (StringUtils.isBlank(edgeConfig.getDstKeyPolicy)) Option.empty else Option(KeyPolicy.withName(edgeConfig.getDstKeyPolicy))
    if (properties.isEmpty) {
      for (elem <- rdd.columns) {
        properties += (elem -> 1)
      }
    }
    rdd.repartition(nebulaConfig.getPartitions).map { row =>
      val values: ListBuffer[Any] = new ListBuffer[Any]()
      for {property <- properties.keySet} {
        values.append(extraValue(row, property))
      }
      val valueStr = values.mkString(",")
      (String.valueOf(extraValue(row, edgeConfig.getSrcIdField)), String.valueOf(extraValue(row, edgeConfig.getDstIdField)), edgeConfig.getRank, valueStr)
    }(Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.LONG, Encoders.STRING))
      .foreachPartition { iterator: Iterator[(String, String, java.lang.Long, String)] =>
        val writer = new NebulaWriter(nebulaOptions = nebulaOptions)
        writer.prepareSpace()
        iterator.grouped(nebulaConfig.getBatchWriteSize).foreach { edges =>
          val values =
            if (edgeConfig.getRank == null || edgeConfig.getRank == 0L) {
              edges
                .map { edge =>
                  (for (source <- edge._1.split(","))
                    yield
                      if (srcKeyPolicy.isEmpty && dstKeyPolicy.isEmpty) {
                        EDGE_VALUE_WITHOUT_RANKING_TEMPLATE
                          .format(source, edge._2, edge._4)
                      } else {
                        val source = if (srcKeyPolicy.isDefined) {
                          srcKeyPolicy.get match {
                            case KeyPolicy.HASH =>
                              ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge._1)
                            case KeyPolicy.UUID =>
                              ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge._1)
                            case _ =>
                              throw new IllegalArgumentException()
                          }
                        } else {
                          edge._1
                        }
                        val target = if (dstKeyPolicy.isDefined) {
                          dstKeyPolicy.get match {
                            case KeyPolicy.HASH =>
                              ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge._2)
                            case KeyPolicy.UUID =>
                              ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge._2)
                            case _ =>
                              throw new IllegalArgumentException()
                          }
                        } else {
                          edge._2
                        }
                        EDGE_VALUE_WITHOUT_RANKING_TEMPLATE_WITH_POLICY
                          .format(source, target, edge._4)
                      }).mkString(",")
                }
                .toList
                .mkString(",")
            } else {
              edges
                .map { edge =>
                  (for (source <- edge._1.split(","))
                    yield
                      if (srcKeyPolicy.isEmpty && dstKeyPolicy.isEmpty) {
                        EDGE_VALUE_TEMPLATE
                          .format(source, edge._2, edge._3, edge._4)
                      } else {
                        val source = srcKeyPolicy.get match {
                          case KeyPolicy.HASH =>
                            ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge._1)
                          case KeyPolicy.UUID =>
                            ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge._1)
                          case _ =>
                            edge._1
                        }
                        val target = dstKeyPolicy.get match {
                          case KeyPolicy.HASH =>
                            ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge._2)
                          case KeyPolicy.UUID =>
                            ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge._2)
                          case _ =>
                            edge._2
                        }
                        EDGE_VALUE_TEMPLATE_WITH_POLICY
                          .format(source, target, edge._3, edge._4)
                      })
                    .mkString(",")
                }
                .toList
                .mkString(",")
            }
          val exec = BATCH_INSERT_TEMPLATE
            .format(DataTypeEnum.EDGE.toString, edgeConfig.getEdge, properties.keySet.mkString(","), values)
          writer.submit(exec)
        }
      }
  }
}
