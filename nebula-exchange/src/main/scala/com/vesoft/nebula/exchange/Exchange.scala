/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.exchange

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import com.vesoft.nebula.exchange.config.{
  ClickHouseConfigEntry,
  Configs,
  DataSourceConfigEntry,
  FileBaseSourceConfigEntry,
  HBaseSourceConfigEntry,
  HiveSourceConfigEntry,
  JanusGraphSourceConfigEntry,
  KafkaSourceConfigEntry,
  MaxComputeConfigEntry,
  MySQLSourceConfigEntry,
  Neo4JSourceConfigEntry,
  PulsarSourceConfigEntry,
  SinkCategory,
  SourceCategory,
  KafkaReloadCategory
}
import com.vesoft.nebula.exchange.processor.{EdgeProcessor, VerticesProcessor, KafkaProcessor}
import com.vesoft.nebula.exchange.reader.{
  CSVReader,
  ClickhouseReader,
  HBaseReader,
  HiveReader,
  JSONReader,
  JanusGraphReader,
  KafkaReader,
  MaxcomputeReader,
  MySQLReader,
  Neo4JReader,
  ORCReader,
  ParquetReader,
  PulsarReader
}
import com.vesoft.nebula.exchange.processor.ReloadProcessor
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import scala.util.control.Breaks

final case class Argument(config: String = "application.conf",
                          hive: Boolean = false,
                          directly: Boolean = false,
                          dry: Boolean = false,
                          reload: String = "")

final case class TooManyErrorsException(private val message: String) extends Exception(message)

/**
  * SparkClientGenerator is a simple spark job used to write data into Nebula Graph parallel.
  */
object Exchange {
  private[this] val LOG = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val PROGRAM_NAME = "Nebula Graph Exchange"
    val options      = Configs.parser(args, PROGRAM_NAME)
    val c: Argument = options match {
      case Some(config) => config
      case _ =>
        LOG.error("Argument parse failed")
        sys.exit(-1)
    }

    val configs = Configs.parse(new File(c.config))
    LOG.info(s"Config ${configs}")

    val session = SparkSession
      .builder()
      .appName(PROGRAM_NAME)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "1")

    for (key <- configs.sparkConfigEntry.map.keySet) {
      session.config(key, configs.sparkConfigEntry.map(key))
    }

    val sparkConf = new SparkConf()
    sparkConf.registerKryoClasses(Array(classOf[com.facebook.thrift.async.TAsyncClientManager]))

    // config hive for sparkSession
    if (c.hive) {
      if (configs.hiveConfigEntry.isEmpty) {
        LOG.info("you don't config hive source, so using hive tied with spark.")
      } else {
        val hiveConfig = configs.hiveConfigEntry.get
        sparkConf.set("spark.sql.warehouse.dir", hiveConfig.warehouse)
        sparkConf
          .set("javax.jdo.option.ConnectionURL", hiveConfig.connectionURL)
          .set("javax.jdo.option.ConnectionDriverName", hiveConfig.connectionDriverName)
          .set("javax.jdo.option.ConnectionUserName", hiveConfig.connectionUserName)
          .set("javax.jdo.option.ConnectionPassword", hiveConfig.connectionPassWord)
      }
    }

    session.config(sparkConf)

    if (c.hive) {
      session.enableHiveSupport()
    }

    val spark = session.getOrCreate()

    // reload for failed import tasks
    if (!c.reload.isEmpty) {
      val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.reload")
      val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.reload")

      val data      = spark.read.text(c.reload)
      val processor = new ReloadProcessor(data, configs, batchSuccess, batchFailure)
      processor.process()
      LOG.info(s"batchSuccess.reload: ${batchSuccess.value}")
      LOG.info(s"batchFailure.reload: ${batchFailure.value}")
      sys.exit(0)
    }

    if (configs.kafkaConfigEntry.isDefined) {
      if (configs.tagsConfig.isEmpty && configs.edgesConfig.isEmpty) {
        throw new IllegalArgumentException("Both tag and edge is empty, nothing to parse")
      }
      val kafkaConfig = configs.kafkaConfigEntry.get
      // handle error
      if (kafkaConfig.reloadEntry.isDefined) {
        val pattern = kafkaConfig.reloadEntry.get.reload
        LOG.info(s"Need to reload kafka data in ${configs.errorConfig.errorPath}, the pattern is $pattern.")
        pattern match {
          case KafkaReloadCategory.ONLYONCE =>
            if (!handleError(spark, configs, configs.errorConfig.errorPath)) {
              LOG.info("Can not reload data successfully")
            }
            LOG.info("Reload kafka data down")
            LOG.info("Kafka reload category is onlyonce, so we shutdown after reload data.")
            spark.close()
            sys.exit(0)
          case KafkaReloadCategory.NEEDRETRY =>
            val retry = kafkaConfig.reloadEntry.get.retry
            LOG.info(s"Retry times: $retry")
            val loop = new Breaks
            var cnt = 0
            loop.breakable(
              while (cnt < retry) {
                if (handleError(spark, configs, configs.errorConfig.errorPath)) {
                  loop.break()
                } else {
                  cnt += 1
                }
              }
            )
            if (cnt == retry) {
              LOG.error("Can not reload data successfully")
            }
            LOG.info("Reload kafka data down")
            LOG.info("Kafka reload category is needtry, so we shutdown after reload data.")
            spark.close()
            sys.exit(0)
          case KafkaReloadCategory.CONTINUE =>
            if (!handleError(spark, configs, configs.errorConfig.errorPath)) {
              LOG.info("Can not reload data successfully")
            }
            LOG.info("Reload data down. Begin consume kakfa data and send it to nebula")
          case _ =>
            throw new IllegalArgumentException("Unknow pattern for kafka reload.")
        }
      }
      LOG.info("Parse tag and edge from kafka value and insert them to nebula")
      // import data inside kafka value to Nebula
      val data = createDataSource(spark, kafkaConfig)
      if (data.isDefined && !c.dry) {
        val processor = new KafkaProcessor(
          spark,
          repartition(data.get, kafkaConfig.partition, SourceCategory.KAFKA),
          configs)
        processor.process()
      } else {
        LOG.info("Unknow error, can not read data from kafka")
      }
      spark.close()
      sys.exit(0)
    }

    // record the failed batch number
    var failures: Long = 0L

    // import tags
    if (configs.tagsConfig.nonEmpty) {
      for (tagConfig <- configs.tagsConfig) {
        LOG.info(s"Processing Tag ${tagConfig.name}")

        val fieldKeys = tagConfig.fields
        LOG.info(s"field keys: ${fieldKeys.mkString(", ")}")
        val nebulaKeys = tagConfig.nebulaFields
        LOG.info(s"nebula keys: ${nebulaKeys.mkString(", ")}")

        val data = createDataSource(spark, tagConfig.dataSourceConfigEntry)
        if (data.isDefined && !c.dry) {
          val startTime = System.currentTimeMillis()
          val batchSuccess =
            spark.sparkContext.longAccumulator(s"batchSuccess.${tagConfig.name}")
          val batchFailure =
            spark.sparkContext.longAccumulator(s"batchFailure.${tagConfig.name}")

          val processor = new VerticesProcessor(
            repartition(data.get, tagConfig.partition, tagConfig.dataSourceConfigEntry.category),
            tagConfig,
            fieldKeys,
            nebulaKeys,
            configs,
            batchSuccess,
            batchFailure)
          processor.process()
          val costTime = ((System.currentTimeMillis() - startTime) / 1000.0).formatted("%.2f")
          LOG.info(s"import for tag ${tagConfig.name} cost time: ${costTime} s")
          if (tagConfig.dataSinkConfigEntry.category == SinkCategory.CLIENT) {
            LOG.info(s"Client-Import: batchSuccess.${tagConfig.name}: ${batchSuccess.value}")
            LOG.info(s"Client-Import: batchFailure.${tagConfig.name}: ${batchFailure.value}")
            failures += batchFailure.value
          } else {
            LOG.info(s"SST-Import: failure.${tagConfig.name}: ${batchFailure.value}")
          }
        }
      }
    } else {
      LOG.warn("Tag is not defined")
    }

    // import edges
    if (configs.edgesConfig.nonEmpty) {
      for (edgeConfig <- configs.edgesConfig) {
        LOG.info(s"Processing Edge ${edgeConfig.name}")

        val fieldKeys = edgeConfig.fields
        LOG.info(s"field keys: ${fieldKeys.mkString(", ")}")
        val nebulaKeys = edgeConfig.nebulaFields
        LOG.info(s"nebula keys: ${nebulaKeys.mkString(", ")}")
        val data = createDataSource(spark, edgeConfig.dataSourceConfigEntry)
        if (data.isDefined && !c.dry) {
          val startTime    = System.currentTimeMillis()
          val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.${edgeConfig.name}")
          val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.${edgeConfig.name}")

          val processor = new EdgeProcessor(
            repartition(data.get, edgeConfig.partition, edgeConfig.dataSourceConfigEntry.category),
            edgeConfig,
            fieldKeys,
            nebulaKeys,
            configs,
            batchSuccess,
            batchFailure
          )
          processor.process()
          val costTime = ((System.currentTimeMillis() - startTime) / 1000.0).formatted("%.2f")
          LOG.info(s"import for edge ${edgeConfig.name} cost time: ${costTime} s")
          if (edgeConfig.dataSinkConfigEntry.category == SinkCategory.CLIENT) {
            LOG.info(s"Client-Import: batchSuccess.${edgeConfig.name}: ${batchSuccess.value}")
            LOG.info(s"Client-Import: batchFailure.${edgeConfig.name}: ${batchFailure.value}")
            failures += batchFailure.value
          } else {
            LOG.info(s"SST-Import: failure.${edgeConfig.name}: ${batchFailure.value}")
          }
        }
      }
    } else {
      LOG.warn("Edge is not defined")
    }

    // reimport for failed tags and edges
    if (failures > 0) {
      handleError(spark, configs, configs.errorConfig.errorPath)
    }
    spark.close()
  }

  /**
    * Create data source for different data type.
    *
    * @param session The Spark Session.
    * @param config  The config.
    * @return
    */
  private[this] def createDataSource(
      session: SparkSession,
      config: DataSourceConfigEntry
  ): Option[DataFrame] = {
    config.category match {
      case SourceCategory.PARQUET =>
        val parquetConfig = config.asInstanceOf[FileBaseSourceConfigEntry]
        LOG.info(s"""Loading Parquet files from ${parquetConfig.path}""")
        val reader = new ParquetReader(session, parquetConfig)
        Some(reader.read())
      case SourceCategory.ORC =>
        val orcConfig = config.asInstanceOf[FileBaseSourceConfigEntry]
        LOG.info(s"""Loading ORC files from ${orcConfig.path}""")
        val reader = new ORCReader(session, orcConfig)
        Some(reader.read())
      case SourceCategory.JSON =>
        val jsonConfig = config.asInstanceOf[FileBaseSourceConfigEntry]
        LOG.info(s"""Loading JSON files from ${jsonConfig.path}""")
        val reader = new JSONReader(session, jsonConfig)
        Some(reader.read())
      case SourceCategory.CSV =>
        val csvConfig = config.asInstanceOf[FileBaseSourceConfigEntry]
        LOG.info(s"""Loading CSV files from ${csvConfig.path}""")
        val reader =
          new CSVReader(session, csvConfig)
        Some(reader.read())
      case SourceCategory.HIVE =>
        val hiveConfig = config.asInstanceOf[HiveSourceConfigEntry]
        LOG.info(s"""Loading from Hive and exec ${hiveConfig.sentence}""")
        val reader = new HiveReader(session, hiveConfig)
        Some(reader.read())
      case SourceCategory.KAFKA => {
        val kafkaConfig = config.asInstanceOf[KafkaSourceConfigEntry]
        LOG.info(s"""Loading from Kafka ${kafkaConfig.server} and subscribe ${kafkaConfig.topic}""")
        val reader = new KafkaReader(session, kafkaConfig)
        Some(reader.read())
      }
      case SourceCategory.NEO4J =>
        val neo4jConfig = config.asInstanceOf[Neo4JSourceConfigEntry]
        LOG.info(s"Loading from neo4j config: ${neo4jConfig}")
        val reader = new Neo4JReader(session, neo4jConfig)
        Some(reader.read())
      case SourceCategory.MYSQL =>
        val mysqlConfig = config.asInstanceOf[MySQLSourceConfigEntry]
        LOG.info(s"Loading from mysql config: ${mysqlConfig}")
        val reader = new MySQLReader(session, mysqlConfig)
        Some(reader.read())
      case SourceCategory.PULSAR =>
        val pulsarConfig = config.asInstanceOf[PulsarSourceConfigEntry]
        LOG.info(s"Loading from pulsar config: ${pulsarConfig}")
        val reader = new PulsarReader(session, pulsarConfig)
        Some(reader.read())
      case SourceCategory.JANUS_GRAPH =>
        val janusGraphSourceConfigEntry = config.asInstanceOf[JanusGraphSourceConfigEntry]
        val reader                      = new JanusGraphReader(session, janusGraphSourceConfigEntry)
        Some(reader.read())
      case SourceCategory.HBASE =>
        val hbaseSourceConfigEntry = config.asInstanceOf[HBaseSourceConfigEntry]
        val reader                 = new HBaseReader(session, hbaseSourceConfigEntry)
        Some(reader.read())
      case SourceCategory.MAXCOMPUTE =>
        val maxComputeConfigEntry = config.asInstanceOf[MaxComputeConfigEntry]
        val reader                = new MaxcomputeReader(session, maxComputeConfigEntry)
        Some(reader.read())
      case SourceCategory.CLICKHOUSE => {
        val clickhouseConfigEntry = config.asInstanceOf[ClickHouseConfigEntry]
        val reader                = new ClickhouseReader(session, clickhouseConfigEntry)
        Some(reader.read())
      }
      case _ => {
        LOG.error(s"Data source ${config.category} not supported")
        None
      }
    }
  }

  /**
    * handle error gql in error path
    *
    * @param spark
    * @param configs
    * @param errorpath
    * @param needStored
    *
    * @return
    */
  private[this] def handleError(spark: SparkSession, configs: Configs, errorpath: String): Boolean = {
    if (ErrorHandler.existError(errorpath)) {
      LOG.info(s"Begin handle error stored in $errorpath")
      // get date need to reload
      val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.reimport")
      val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.reimport")
      val data         = spark.read.text(errorpath)
      data.cache()

      // rename the reload file
      ErrorHandler.rename(errorpath)

      val count     = data.count()
      val startTime = System.currentTimeMillis()
      val processor = new ReloadProcessor(data, configs, batchSuccess, batchFailure)
      processor.process()
      val costTime = ((System.currentTimeMillis() - startTime) / 1000.0).formatted("%.2f")
      LOG.info(s"reimport ngql count: ${count}, cost time: ${costTime}")
      LOG.info(s"batchSuccess.reimport: ${batchSuccess.value}")
      LOG.info(s"batchFailure.reimport: ${batchFailure.value}")

      // clear stale reload file generated during last reload period.
      // Only store reload file created during this reload process period.
      LOG.info(s"clear error files in $errorpath except file which start with reload")
      ErrorHandler.clear(errorpath)
      batchFailure.value == 0
    } else {
      LOG.info(s"There is no error stored in $errorpath. Skip error handle")
      true
    }
  }
  /**
    * Repartition the data frame using the specified partition number.
    *
    * @param frame
    * @param partition
    * @return
    */
  private[this] def repartition(frame: DataFrame,
                                partition: Int,
                                sourceCategory: SourceCategory.Value): DataFrame = {
    if (partition > 0 && !CheckPointHandler.checkSupportResume(sourceCategory)) {
      frame.repartition(partition).toDF
    } else {
      frame
    }
  }
}
