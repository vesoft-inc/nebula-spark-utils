/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.fix.nebula

import com.vesoft.nebula.connector.NebulaOptions
import com.vesoft.nebula.connector.fix.config.{NebulaConfig, VertexConfig}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, Encoders}

import scala.collection.mutable.ListBuffer


/**
  * @Description NebulaVertexWriter is used for 
  * @author huangzhaolai-jk
  * @Date 2021/8/2 - 14:27 
  * @version 1.0.0
  */
class NebulaVertexWriter(nebulaOptions: NebulaOptions, vertexConfig: VertexConfig, nebulaConfig: NebulaConfig) extends NebulaBaseWriter {

  private var properties: Map[String, Integer] = Map()

  override def write(rdd: DataFrame): Unit = {
    val keyPolicy = if (StringUtils.isBlank(vertexConfig.getKeyPolicy)) Option.empty else Option(KeyPolicy.withName(vertexConfig.getKeyPolicy))
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
      (String.valueOf(extraValue(row, vertexConfig.getIdField)), valueStr)
    }(Encoders.tuple(Encoders.STRING, Encoders.STRING))
      .foreachPartition { iterator: Iterator[(String, String)] =>
        val writer = new NebulaWriter(nebulaOptions)
        writer.prepareSpace()
        iterator.grouped(nebulaConfig.getBatchWriteSize).foreach { tags =>
          val exec = BATCH_INSERT_TEMPLATE.format(
            DataTypeEnum.VERTEX.toString,
            vertexConfig.getTag,
            properties.keySet.mkString(", "),
            tags
              .map { tag =>
                if (keyPolicy.isEmpty) {
                  INSERT_VALUE_TEMPLATE.format(tag._1, tag._2)
                } else {
                  keyPolicy.get match {
                    case KeyPolicy.HASH =>
                      INSERT_VALUE_TEMPLATE_WITH_POLICY
                        .format(KeyPolicy.HASH.toString, tag._1, tag._2)
                    case KeyPolicy.UUID =>
                      INSERT_VALUE_TEMPLATE_WITH_POLICY
                        .format(KeyPolicy.UUID.toString, tag._1, tag._2)
                    case _ => throw new IllegalArgumentException
                  }
                }
              }
              .mkString(",")
          )
          writer.submit(exec)
        }
      }
  }


}
