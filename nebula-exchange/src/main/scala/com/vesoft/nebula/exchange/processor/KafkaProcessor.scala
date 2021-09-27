/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.processor

import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, ByteOrder}

import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.encoder.NebulaCodecImpl
import com.vesoft.nebula.exchange.{
  ErrorHandler,
  GraphProvider,
  KeyPolicy,
  MetaProvider,
  Vertex,
  Vertices,
  Edge,
  Edges,
  MultiInputs,
  VidType
}
import com.vesoft.nebula.exchange.config.{
  Configs,
  FileBaseSinkConfigEntry,
  SinkCategory,
  StreamingDataSourceConfigEntry,
  TagConfigEntry,
  EdgeConfigEntry
}
import com.vesoft.nebula.exchange.utils.NebulaUtils.DEFAULT_EMPTY_VALUE
import com.vesoft.nebula.exchange.utils.{HDFSUtils, NebulaUtils}
import com.vesoft.nebula.exchange.writer.{NebulaGraphClientWriter, NebulaSSTWriter}
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.alibaba.fastjson.{JSON, JSONObject}
import java.beans.Encoder

/**
  * parse tags/edges from kafka value
  *
  * @param spark
  * @param data
  * @param configs
  */
class KafkaProcessor(spark: SparkSession,
                     dataFrame: DataFrame,
                     configs: Configs)
    extends Processor {

  @transient
  private[this] lazy val LOG    =  Logger.getLogger(this.getClass)
  private[this] val kafkaConfig = configs.kafkaConfigEntry.get

  private def getMetaProvider(): MetaProvider = {
    val address         = configs.databaseConfig.getMetaAddress
    val timeout         = configs.connectionConfig.timeout
    val retry           = configs.connectionConfig.retry
    new MetaProvider(address, timeout, retry)
  }

  private def parseFieldTypeMap(space: String,
                                metaProvider: MetaProvider):
                                Map[String, Map[String, Int]] = {
    val tagSchemaMap: mutable.Map[String, Map[String, Int]] = mutable.HashMap[String, Map[String, Int]]()
    val edgeSchemaMap: mutable.Map[String, Map[String, Int]] = mutable.HashMap[String, Map[String, Int]]()
    var tagStr = "    Empty vertice \n"
    var edgeStr = "    Empty edge \n"
    if (configs.tagsConfig.nonEmpty) {
      for (tagConfig <- configs.tagsConfig) {
        val fieldTypeMap = NebulaUtils.getDataSourceFieldType(tagConfig, space, metaProvider)
        tagSchemaMap.put(tagConfig.name, fieldTypeMap)
      }
      tagStr = tagSchemaMap.toList
                              .map(t => "    tag name: " + t._1.toString + " , field type: " +t._2.mkString(", "))
                              .mkString("\n")
    }
    if (configs.edgesConfig.nonEmpty) {
      for (edgeConfig <- configs.edgesConfig) {
        val fieldTypeMap = NebulaUtils.getDataSourceFieldType(edgeConfig, space, metaProvider)
        edgeSchemaMap.put(edgeConfig.name, fieldTypeMap)
      }
      edgeStr = edgeSchemaMap.toList
                             .map(t => "    edge name: " + t._1.toString + " , field type: " + t._2.mkString(", "))
                             .mkString("\n")
    }
    val str = "The map of tag/edge field and its corresponding data type is show below: \n" +
              "Vertices: \n" + tagStr +
              "\nEdges: \n" + edgeStr

    LOG.info(str)
    val sourceSchemaMap = tagSchemaMap++edgeSchemaMap
    sourceSchemaMap.toMap
  }

  private def getAndCheckVID(vertexID: String,
                             noPolicy: Boolean,
                             isVidStringType: Boolean,
                             id: String): String = {
    def checkVid(vid: String): String = {
      if (noPolicy) {
        // process string type vid
        if (isVidStringType) {
          NebulaUtils.escapeUtil(vid).mkString("\"", "", "\"")
        } else {
          // process int type vid
          assert(NebulaUtils.isNumic(vid),
                  s"space vidType is int, but your $id $vid is not numeric.")
          vid
        }
      } else {
        assert(!isVidStringType,
                "only int vidType can use policy, but your vidType is FIXED_STRING.")
        vid
      }
    }

    if (vertexID.equals(DEFAULT_EMPTY_VALUE)) {
      checkVid("")
    } else
      checkVid(vertexID)
  }

  private def parseVertexFromJSON(jsonObj: JSONObject,
                                  getFieldTypeByNameMap: Map[String, Map[String, Int]],
                                  isVidStringType: Boolean):
                                  Option[Map[String, Vertex]] = {
    if (configs.tagsConfig.nonEmpty) {
      val vertexMap: mutable.Map[String, Vertex] = mutable.HashMap[String, Vertex]()
      for (tagConfig <- configs.tagsConfig) {
        val vertexID = getAndCheckVID(jsonObj.getString(tagConfig.vertexField),
                                      tagConfig.vertexPolicy.isEmpty,
                                      isVidStringType,
                                      "vertex id")
        val values = for {
          property <- tagConfig.fields if property.trim.length != 0
        } yield extraValueForClientFromJSON(jsonObj, property, getFieldTypeByNameMap(tagConfig.name))
        val vertex = Vertex(vertexID, values)
        vertexMap.put(tagConfig.name, vertex)
      }
      Some(vertexMap.toMap)
    } else
      None
  }

  private def parseEdgeFromJSON(jsonObj: JSONObject,
                                getFieldTypeByNameMap: Map[String, Map[String, Int]],
                                isVidStringType: Boolean):
                                Option[Map[String, Edge]] = {
    if (configs.edgesConfig.nonEmpty) {
      val edgeMap: mutable.Map[String, Edge] = mutable.HashMap[String, Edge]()
      for (edgeConfig <- configs.edgesConfig) {
        var sourceField = getAndCheckVID(jsonObj.getString(edgeConfig.sourceField),
                                         edgeConfig.sourcePolicy.isEmpty,
                                         isVidStringType,
                                         "srcId")
        var targetField = getAndCheckVID(jsonObj.getString(edgeConfig.targetField),
                                         edgeConfig.targetPolicy.isEmpty,
                                         isVidStringType,
                                         "dstId")
        val values = for {
          property <- edgeConfig.fields if property.trim.length != 0
        } yield extraValueForClientFromJSON(jsonObj, property, getFieldTypeByNameMap(edgeConfig.name))

        var edge = if (edgeConfig.rankingField.isDefined) {
          val ranking = jsonObj.getString(edgeConfig.rankingField.get)
          assert(NebulaUtils.isNumic(ranking), s"Not support non-Numeric type for ranking field")
          Edge(sourceField, targetField, Some(ranking.toLong), values)
        } else {
          Edge(sourceField, targetField, None, values)
        }
        edgeMap.put(edgeConfig.name, edge)
      }
      Some(edgeMap.toMap)
    } else
      None
  }

  private def processEachPartition(iterator: Iterator[MultiInputs]): Unit = {
    val graphProvider =
      new GraphProvider(configs.databaseConfig.getGraphAddress, configs.connectionConfig.timeout)

    var batchSuccess = 0
    var batchFailure = 0

    var writer = new NebulaGraphClientWriter(configs.databaseConfig,
                                              configs.userConfig,
                                              configs.rateConfig,
                                              graphProvider)
    writer.prepare()
    var errorBufferMap: mutable.Map[String, ArrayBuffer[String]] = mutable.HashMap[String, ArrayBuffer[String]]()
    if (configs.tagsConfig.nonEmpty) {
      for (tagConfig <- configs.tagsConfig) {
        errorBufferMap.put(tagConfig.name, ArrayBuffer[String]())
      }
    }
    if (configs.edgesConfig.nonEmpty) {
      for (edgeConfig <- configs.edgesConfig) {
          errorBufferMap.put(edgeConfig.name, ArrayBuffer[String]())
      }
    }

    val startTime = System.currentTimeMillis
    iterator.grouped(kafkaConfig.batch).foreach { multiInputs =>
      val inputs = multiInputs.toList
      if (kafkaConfig.verbose) {
        LOG.info(inputs.mkString("\n"))
      }
      if (configs.tagsConfig.nonEmpty) {
        for (tagConfig <- configs.tagsConfig) {
          val vertexList: List[Vertex] = inputs.map(_.vertices.get(tagConfig.name))
          val vertices                 = Vertices(tagConfig.nebulaFields, vertexList, tagConfig.vertexPolicy)
          val failStatement            = writer.writeVertices(vertices, tagConfig.name)
          if (failStatement == null) {
            batchSuccess += 1
          } else {
            errorBufferMap(tagConfig.name).append(failStatement)
            batchFailure += 1
          }
        }
      }

      if (configs.edgesConfig.nonEmpty) {
        for (edgeConfig <- configs.edgesConfig) {
          val edgeList: List[Edge] = inputs.map(_.edges.get(edgeConfig.name))
          val edges                = Edges(edgeConfig.nebulaFields, edgeList, edgeConfig.sourcePolicy, edgeConfig.targetPolicy)
          val failStatement        = writer.writeEdges(edges, edgeConfig.name)
          if (failStatement == null) {
            batchSuccess += 1
          } else {
            errorBufferMap(edgeConfig.name).append(failStatement)
            batchFailure += 1
          }
        }
      }
    }

    writer.close()

    errorBufferMap.foreach {
      case (name, errorBuffer) =>
        if (errorBuffer.nonEmpty) {
          ErrorHandler.save(
            errorBuffer,
            s"${configs.errorConfig.errorPath}/${name}.${TaskContext.getPartitionId()}")
          errorBuffer.clear()
        }
    }

    LOG.info(s"kafka ${kafkaConfig.topic} import in spark partition ${TaskContext
      .getPartitionId()} cost ${System.currentTimeMillis() - startTime} ms")
    LOG.info(s"In this partition batchSuccess: ${batchSuccess.toString()}")
    LOG.info(s"In this partition batchFailure: ${batchFailure.toString()}")
    graphProvider.close()
  }

  override def process(): Unit = {
    for (edgeConfig <- configs.edgesConfig) {
      if (edgeConfig.isGeo) {
        throw new IllegalArgumentException("Do not support Geo for edge now. Please indicate source vertex of edge")
      }
    }
    val space                 = configs.databaseConfig.space
    val metaProvider          = getMetaProvider()
    val getFieldTypeByNameMap = parseFieldTypeMap(space, metaProvider)
    val isVidStringType       = metaProvider.getVidType(space) == VidType.STRING

    import spark.implicits._
    val data = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    val multiInputs = data.map(_._2).map(
        value => {
          if (kafkaConfig.verbose) {
            LOG.info(value)
          }
          var jsonObj = JSON.parseObject("{}")
          try {
            jsonObj = JSON.parseObject(value)
          }
          catch {
            case e: Exception => {
              e.printStackTrace()
              throw new IllegalArgumentException("Can not parse json string from value field of kafka.")
            }
          }
          val multiVertex = parseVertexFromJSON(jsonObj, getFieldTypeByNameMap, isVidStringType)
          val multiEdge = parseEdgeFromJSON(jsonObj, getFieldTypeByNameMap, isVidStringType)
          MultiInputs(multiVertex, multiEdge)
        }
    )(Encoders.kryo[MultiInputs])
    val streamingDataSourceConfig =
      kafkaConfig.asInstanceOf[StreamingDataSourceConfigEntry]
    multiInputs.writeStream
      .foreachBatch((multiInputSet, batchId) => {
        LOG.info(s"${kafkaConfig.topic} start batch ${batchId}.")
        multiInputSet.foreachPartition(processEachPartition _)
      })
      .trigger(Trigger.ProcessingTime(s"${streamingDataSourceConfig.intervalSeconds} seconds"))
      .start()
      .awaitTermination()
  }
}
