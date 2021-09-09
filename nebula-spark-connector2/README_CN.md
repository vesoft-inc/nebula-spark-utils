# 欢迎使用 Nebula Spark Connector 2.0
## 介绍

Nebula Spark Connector 2.0 仅支持 Nebula Graph 2.x。如果您正在使用 Nebula Graph v1.x，请使用 [Nebula Spark Connector v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools)。

## 如何编译

1. 编译打包 Nebula Spark Connector 2.0。

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-spark-utils/nebula-spark-connector
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```

    编译打包完成后，可以在 nebula-spark-utils/nebula-spark-connector/target/ 目录下看到 nebula-spark-connector-2.0.0.jar 文件。

## 特性

* 提供了更多连接配置项，如超时时间、连接重试次数、执行重试次数
* 提供了更多数据配置项，如写入数据时是否将 vertexId 同时作为属性写入、是否将 srcId、dstId、rank 等同时作为属性写入
* Spark Reader 支持无属性读取，支持全属性读取
* Spark Reader 支持将 Nebula Graph 数据读取成 Graphx 的 VertexRD 和 EdgeRDD，支持非 Long 型 vertexId
* Nebula Spark Connector 2.0 统一了 SparkSQL 的扩展数据源，统一采用 DataSourceV2 进行 Nebula Graph 数据扩展
* Nebula Spark Connector 2.1.0 增加了 UPDATE 写入模式，相关说明参考[Update Vertex](https://docs.nebula-graph.com.cn/2.0.1/3.ngql-guide/12.vertex-statements/2.update-vertex/) 。

## 使用说明

  ````
private def getNebulaWriter(config: GraphDataMigrateConfig, beanTypeEnum: BeanTypeEnum): NebulaBaseWriter = {
    val nebulaConfig = config.getNebulaConfig
    var paramMap: scala.Predef.Map[String, String] = Map()
    paramMap += (NebulaOptions.GRAPH_ADDRESS -> nebulaConfig.getGraphAddress)
    paramMap += (NebulaOptions.META_ADDRESS -> nebulaConfig.getMetaAddress)
    paramMap += (NebulaOptions.BATCH -> nebulaConfig.getBatchWriteSize.toString)
    paramMap += (NebulaOptions.SPACE_NAME -> nebulaConfig.getSpace)
    paramMap += (NebulaOptions.CONNECTION_RETRY -> nebulaConfig.getConnectionRetryTime.toString)
    paramMap += (NebulaOptions.USER_NAME -> nebulaConfig.getUserName)
    paramMap += (NebulaOptions.PASSWD -> nebulaConfig.getPassword)
    paramMap += (NebulaOptions.RATE_LIMIT -> nebulaConfig.getRateLimit)
    paramMap += (NebulaOptions.RATE_TIME_OUT -> nebulaConfig.getRateTimeOut)
    val graphClazz = invokeEntityClass(beanTypeEnum)
    val label = getGraphSchemaName(graphClazz)
    if (BeanTypeEnum.VERTEX_BEAN.equals(beanTypeEnum)) {
      paramMap += (NebulaOptions.TYPE -> DataTypeEnum.VERTEX.toString)
      paramMap += (NebulaOptions.LABEL -> label)
      val caseInsensitiveMap = CaseInsensitiveMap(paramMap)
      val options = new NebulaOptions(caseInsensitiveMap)(OperaType.WRITE)
      val vertexConfig = new VertexConfig()
      vertexConfig.setTag(label)
      vertexConfig.setIdField(getVertexIdField(graphClazz))
      new NebulaVertexWriter(options, vertexConfig, nebulaConfig)
    } else {
      paramMap += (NebulaOptions.TYPE -> DataTypeEnum.EDGE.toString)
      paramMap += (NebulaOptions.LABEL -> label)
      val caseInsensitiveMap = CaseInsensitiveMap(paramMap)
      val options = new NebulaOptions(caseInsensitiveMap)(OperaType.WRITE)
      val edgeConfig = new EdgeConfig()
      edgeConfig.setEdge(label)
      edgeConfig.setSrcIdField(getSrcIdField(graphClazz))
      edgeConfig.setDstIdField(getDstIdField(graphClazz))
      new NebulaEdgeWriter(options, edgeConfig, nebulaConfig)
    }
  }
    val writer = getNebulaWriter(config, beanType)
    writer.write(rdd)
````
  
  得到 Graphx 的 Graph 之后，可以根据 [Nebula-Spark-Algorithm](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/nebula-algorithm) 的示例在 Graphx 框架中进行算法开发。

更多使用示例请参考 [Example](https://github.com/vesoft-inc/nebula-spark-utils/tree/master/example/src/main/scala/com/vesoft/nebula/examples/connector) 。

## 贡献

Nebula Spark Connector 2.0 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与：

- 前往 [Nebula Graph 论坛](https://discuss.nebula-graph.com.cn/ "点击前往“Nebula Graph 论坛") 上参与 Issue 讨论，如答疑、提供想法或者报告无法解决的问题
- 撰写或改进文档
- 提交优化代码
