/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange.processor

import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, ByteOrder}

import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.encoder.{NebulaCodec, NebulaCodecImpl}
import com.vesoft.nebula.exchange.{
  ErrorHandler,
  GraphProvider,
  KeyPolicy,
  MetaProvider,
  Vertex,
  Vertices,
  VidType
}
import com.vesoft.nebula.exchange.config.{
  CSVSinkConfigEntry,
  Configs,
  FileBaseSinkConfigEntry,
  FileDataSinkConfigEntry,
  SinkCategory,
  StreamingDataSourceConfigEntry,
  TagConfigEntry
}
import com.vesoft.nebula.exchange.utils.{HDFSUtils, NebulaUtils}
import com.vesoft.nebula.exchange.writer.{CsvWriter, NebulaGraphClientWriter, NebulaSSTWriter}
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.log4j.Logger
import com.google.common.net.HostAndPort
import org.apache.spark.TaskContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  *
  * @param data
  * @param tagConfig
  * @param fieldKeys
  * @param nebulaKeys
  * @param config
  * @param batchSuccess
  * @param batchFailure
  */
class VerticesProcessor(data: DataFrame,
                        tagConfig: TagConfigEntry,
                        fieldKeys: List[String],
                        nebulaKeys: List[String],
                        config: Configs,
                        batchSuccess: LongAccumulator,
                        batchFailure: LongAccumulator)
    extends Processor {

  @transient
  private[this] lazy val LOG = Logger.getLogger(this.getClass)

  private def processEachPartition(iterator: Iterator[Vertex]): Unit = {
    val graphProvider = new GraphProvider(config.databaseConfig.getGraphAddress)

    val writer = new NebulaGraphClientWriter(config.databaseConfig,
                                             config.userConfig,
                                             config.connectionConfig,
                                             config.executionConfig.retry,
                                             config.rateConfig,
                                             tagConfig,
                                             graphProvider)

    val errorBuffer = ArrayBuffer[String]()

    writer.prepare()
    // batch write tags
    val startTime = System.currentTimeMillis
    iterator.grouped(tagConfig.batch).foreach { vertex =>
      val vertices      = Vertices(nebulaKeys, vertex.toList, tagConfig.vertexPolicy)
      val failStatement = writer.writeVertices(vertices)
      if (failStatement == null) {
        batchSuccess.add(1)
      } else {
        errorBuffer.append(failStatement)
        batchFailure.add(1)
      }
    }
    if (errorBuffer.nonEmpty) {
      ErrorHandler.save(
        errorBuffer,
        s"${config.errorConfig.errorPath}/${tagConfig.name}.${TaskContext.getPartitionId()}")
      errorBuffer.clear()
    }
    LOG.info(
      s"spark partition for vertex cost time:" +
        s"${TaskContext.getPartitionId()}-${System.currentTimeMillis() - startTime}")
    writer.close()
    graphProvider.close()
  }

  override def process(): Unit = {

    var address: List[HostAndPort]     = null
    var space: String                  = null
    var metaProvider: MetaProvider     = null
    var fieldTypeMap: Map[String, Int] = null
    var isVidStringType: Boolean       = false
    var partitionNum: Int              = 0

    if (tagConfig.dataSinkConfigEntry.category != SinkCategory.CSV) {
      address = config.databaseConfig.getMetaAddress
      space = config.databaseConfig.space

      metaProvider = new MetaProvider(address)
      fieldTypeMap = NebulaUtils.getDataSourceFieldType(tagConfig, space, metaProvider)
      isVidStringType = metaProvider.getVidType(space) == VidType.STRING
      partitionNum = metaProvider.getPartNumber(space)
    }

    if (tagConfig.dataSinkConfigEntry.category == SinkCategory.SST) {
      val fileBaseConfig = tagConfig.dataSinkConfigEntry.asInstanceOf[FileBaseSinkConfigEntry]
      val namenode       = fileBaseConfig.fsName.orNull
      val spaceName      = config.databaseConfig.space
      val tagName        = tagConfig.name

      val spaceVidLen = metaProvider.getSpaceVidLen(space)
      val tagItem     = metaProvider.getTagItem(space, tagName)

      data
        .mapPartitions { iter =>
          iter.map { row =>
            val index: Int       = row.schema.fieldIndex(tagConfig.vertexField)
            var vertexId: String = row.get(index).toString

            if (tagConfig.vertexPolicy.isDefined) {
              tagConfig.vertexPolicy.get match {
                case KeyPolicy.HASH =>
                  vertexId = MurmurHash2
                    .hash64(vertexId.getBytes(), vertexId.getBytes().length, 0xc70f6907)
                    .toString
                case KeyPolicy.UUID =>
                  throw new UnsupportedOperationException("do not support uuid yet")
                case _ =>
                  throw new IllegalArgumentException(
                    s"policy ${tagConfig.vertexPolicy.get} is invalidate")
              }
            }

            val hostAddrs: ListBuffer[HostAddress] = new ListBuffer[HostAddress]
            for (addr <- address) {
              hostAddrs.append(new HostAddress(addr.getHostText, addr.getPort))
            }

            val partitionId = NebulaUtils.getPartitionId(spaceName, vertexId, partitionNum)

            val codec = new NebulaCodecImpl()
            val vertexKey =
              codec.vertexKey(spaceVidLen, partitionId, vertexId.getBytes, tagItem.getTag_id)
            val values = for {
              property <- fieldKeys if property.trim.length != 0
            } yield
              extraValueForSST(row, property, fieldTypeMap)
                .asInstanceOf[AnyRef]
            val vertexValue = codec.encodeTag(tagItem, nebulaKeys.asJava, values.asJava)
            (vertexKey, vertexValue)
          }
        }(Encoders.tuple(Encoders.BINARY, Encoders.BINARY))
        .toDF("key", "value")
        .sortWithinPartitions("key")
        .foreachPartition { iterator: Iterator[Row] =>
          val taskID                  = TaskContext.get().taskAttemptId()
          var writer: NebulaSSTWriter = null
          var currentPart             = -1
          val localPath               = fileBaseConfig.localPath
          val remotePath              = fileBaseConfig.remotePath
          try {
            iterator.foreach { vertex =>
              val key   = vertex.getAs[Array[Byte]](0)
              val value = vertex.getAs[Array[Byte]](1)
              var part = ByteBuffer
                .wrap(key, 0, 4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt >> 8
              if (part <= 0) {
                part = part + partitionNum
              }

              if (part != currentPart) {
                if (writer != null) {
                  writer.close()
                  val localFile = s"$localPath/$currentPart-$taskID.sst"
                  HDFSUtils.upload(localFile,
                                   s"$remotePath/${currentPart}/$currentPart-$taskID.sst",
                                   namenode)
                  Files.delete(Paths.get(localFile))
                }
                currentPart = part
                val tmp = s"$localPath/$currentPart-$taskID.sst"
                writer = new NebulaSSTWriter(tmp)
                writer.prepare()
              }
              writer.write(key, value)
            }
          } finally {
            if (writer != null) {
              writer.close()
              val localFile = s"$localPath/$currentPart-$taskID.sst"
              HDFSUtils.upload(localFile,
                               s"$remotePath/${currentPart}/$currentPart-$taskID.sst",
                               namenode)
              Files.delete(Paths.get(localFile))
            }
          }
        }
    } else if (tagConfig.dataSinkConfigEntry.category == SinkCategory.CLIENT) {
      val vertices = data
        .map { row =>
          val vertexID = {
            val index = row.schema.fieldIndex(tagConfig.vertexField)
            assert(index >= 0 && row.get(index) != null,
                   s"vertexId must exist and cannot be null, your row data is $row")
            val value = row.get(index).toString
            if (tagConfig.vertexPolicy.isEmpty) {
              // process string type vid
              if (isVidStringType) {
                NebulaUtils.escapeUtil(value).mkString("\"", "", "\"")
              } else {
                // process int type vid
                assert(NebulaUtils.isNumic(value),
                       s"space vidType is int, but your vertex id $value is not numeric.")
                value
              }
            } else {
              assert(!isVidStringType,
                     "only int vidType can use policy, but your vidType is FIXED_STRING.")
              value
            }
          }

          val values = for {
            property <- fieldKeys if property.trim.length != 0
          } yield extraValueForClient(row, property, fieldTypeMap)
          Vertex(vertexID, values)
        }(Encoders.kryo[Vertex])

      // streaming write
      if (data.isStreaming) {
        val streamingDataSourceConfig =
          tagConfig.dataSourceConfigEntry.asInstanceOf[StreamingDataSourceConfigEntry]
        vertices.writeStream
          .foreachBatch((vertexSet, batchId) => {
            LOG.info(s"${tagConfig.name} tag start batch ${batchId}.")
            vertexSet.foreachPartition(processEachPartition _)
          })
          .trigger(Trigger.ProcessingTime(s"${streamingDataSourceConfig.intervalSeconds} seconds"))
          .start()
          .awaitTermination()
      } else
        vertices.foreachPartition(processEachPartition _)
    } else {
      val fileSinkConfigEntry = tagConfig.dataSinkConfigEntry.asInstanceOf[CSVSinkConfigEntry]
      val path: String        = fileSinkConfigEntry.path
      val writer              = new CsvWriter(path)
      writer.write(data)
    }
  }
}
