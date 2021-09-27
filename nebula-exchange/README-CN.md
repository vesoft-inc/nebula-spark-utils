# 欢迎使用 Nebula Exchange 2.0         
[English](https://github.com/vesoft-inc/nebula-spark-utils/blob/master/nebula-exchange/README.md)

Nebula Exchange 2.0（简称为 Exchange 2.0）是一款 Apache Spark&trade; 应用，用于在分布式环境中将集群中的数据批量迁移到 Nebula Graph 中，能支持多种不同格式的批式数据和流式数据的迁移。

Exchange 2.0 仅支持 Nebula Graph 2.x。

如果您正在使用 Nebula Graph v1.x，请使用 [Nebula Exchange v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/exchange) ，或参考 Exchange 1.0 的使用文档[《Nebula Exchange 用户手册》](https://docs.nebula-graph.com.cn/nebula-exchange/about-exchange/ex-ug-what-is-exchange/ "点击前往 Nebula Graph 网站")。

## 如何获取

1. 编译打包最新的 Exchange。

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-spark-utils.git
    $ cd nebula-spark-utils/nebula-exchange
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```

    编译打包完成后，可以在 nebula-spark-utils/nebula-exchange/target/ 目录下看到 nebula-exchange-2.0-SNAPSHOT.jar 文件。
2. 在 Maven 远程仓库下载
    
    https://repo1.maven.org/maven2/com/vesoft/nebula-exchange/
## 使用说明

特性 & 注意事项：

*1. Nebula Graph 2.0 支持 String 类型和 Integer 类型的点 id 。*

*2. Exchange 2.0 新增 null、Date、DateTime、Time 类型数据的导入（ DateTime 是 UTC 时区，非 Local time）。*

*3. Exchange 2.0 支持 Hive on Spark 以外的 Hive 数据源，需在配置文件中配置 Hive 源，具体配置示例参考 [application.conf](https://github.com/vesoft-inc/nebula-spark-utils/tree/master/nebula-exchange/src/main/resources/application.conf) 中 Hive 的配置。*

*4. Exchange 2.0 将导入失败的 INSERT 语句进行落盘，存于配置文件的 error/output 路径中。*

*5. 配置文件参考 [application.conf](https://github.com/vesoft-inc/nebula-spark-utils/tree/master/nebula-exchange/src/main/resources/application.conf )。*

*6. Exchange 2.0 的导入命令：*
```
$SPARK_HOME/bin/spark-submit --class com.vesoft.nebula.exchange.Exchange --master local nebula-exchange-2.0.0.jar -c /path/to/application.conf
```
如果数据源有HIVE，则导入命令最后还需要加 `-h` 表示启用HIVE数据源。

注：在Yarn-Cluster模式下提交 Exchange，请使用如下提交命令：
```
$SPARK_HOME/bin/spark-submit --class com.vesoft.nebula.exchange.Exchange \
--master yarn-cluster \
--files application.conf \
--conf spark.driver.extraClassPath=./ \
--conf spark.executor.extraClassPath=./ \
nebula-exchange-2.0.0.jar \
-c application.conf
```

关于 Nebula Exchange 的更多说明，请参考 Exchange 2.0 的[使用手册](https://docs.nebula-graph.com.cn/2.0.1/nebula-exchange/about-exchange/ex-ug-what-is-exchange/) 。

## Nebula Exchange 2.5 添加新功能：导入kafka value字段数据到Nebula Graph

主要修改如下：

*1. kafka使用简介*

*2. kafka配置说明*

*3. kafka导入限制*

*4. kafka数据重载*

### kafka使用简介

本功能主要解决从kafka获取数据并导入到Nebula的问题。具体来说，当数据源为kafka时，会默认从kafka的value字段中解析出配置文件里定义的多个tag，多个edge或者多个tag和edge，并将其以client的方式导入到Nebula中。  

### kafka配置说明
由于kafka是流式数据，所以当数据源是kafka时，程序只能一直接收来自kafka的数据，不能进行数据源的切换。因此在本次更新中，我们将kafka的配置项单独列出来，当定义了kafka配置项时，表明配置文件里的tag和edge都从kafka的value字段中解析出来。配置项示例如下所示:
```
  kafka: {
    # 对于kafka，必须在这里设置相关参数，其他源（如hive等）在tag内部指定即可
    # 只能从kafka的value（必须是json字符串）中解析相关数据
    # 只能以client模式将kafka数据导入nebula中

    # Kafka服务器地址。
    # 可以多个。
    service: "kafka-server1,kafka-server2"
    # 消息类别。
    # 只能一个。
    topic: "kafka-topic"
    # 单次写入 Nebula Graph 的最大点数据量。
    batch: 50
    # Spark 分区数量
    partition: 10
    # 读取消息的间隔。单位：秒。
    interval.seconds: 30

    # 如果不需要重新加载未成功执行的ngql语句（保存在error.output路径下）那么这里可以注释掉忽略。
    # 如果需要加载，请确保这些语句没有语义和语法错误。
    # reload有三种模式：
    #       continue:重载一次保存在error.output下的数据，不管执行成功与否。重载完后会继续消费kafka消息。
    #       onlyonce:重载一次保存在error.output下的数据，不管执行成功与否。重载完后退出程序不消费kafka消息。
    #       needretry:一直重载保存在error.output下的数据，直到全部重载成功或者达到retry次数限制。若不定义retry，则默认为3。执行完后退出程序不消费kafka消息。
    # reload: continue
    # retry: 3

    # 是否将kafka中的value字段和从value中解析出来的tag/edge数据打印到log里，若注释掉，则默认是false。
    verbose: true
  }

  tags: [
    {
      # Nebula Graph中对应的标签名称。
      name: tagName

      # 在fields里指定player表中的列名称，其对应的value会作为Nebula Graph中指定属性。
      # fields和nebula.fields里的配置必须一一对应。
      # 如果需要指定多个列名称，用英文逗号（,）隔开。

      # fields中的列名称必须存在于kafka value的json里，否则解析失败。
      # 记录在kafka value字段里的json数据必须都是string/long/int类型。
      fields: [col1, col2]
      nebula.fields: [col1, col2]

      # 指定表中某一列数据为Nebula Graph中点VID的来源。必须唯一，否则vid相同的数据会被覆盖。
      vertex:{
          field:vid
      }
    }
  ]

  edges: [
    {
      # Nebula Graph中对应的边类型名称。
      name: edgeName

      # 在fields里指定follow表中的列名称，其对应的value会作为Nebula Graph中指定属性。
      # fields和nebula.fields里的配置必须一一对应。
      # 如果需要指定多个列名称，用英文逗号（,）隔开。

      # fields中的列名称必须存在于kafka value的json里，否则解析失败。
      # 记录在kafka value字段里的json数据必须都是string/long/int类型。    
      fields: [col1]
      nebula.fields: [col1]

      # 在source里，将follow表中某一列作为边的起始点数据源。必须唯一，否则vid相同的数据会被覆盖。
      # 在target里，将follow表中某一列作为边的目的点数据源。必须唯一，否则vid相同的数据会被覆盖。
      # 注意，相同schema且soure/target相同的边必须指定ranking。
      source:{
          field:src
      }
      target:{
          field:dst
      }
    }
  ]
```  

值得注意的是，kafka可以从多个server处消费数据，但是只能消费一个topic的数据。此外，当数据源不是kafka时，请将kafka项注释掉，否则一旦定义了kafka配置项，就会默认从kafka处消费数据，此时tag/edge内部的type.source失效。  

### kafka导入限制
-   kafka模式默认从kafka的value字段解析数据，此时kafka的其他字段，如key,offset,partition等都会抛弃。
-   value字段必须是json字符串，且每个field的值都必须是string或者int/long类型。程序会根据在Nebula schema中定义的数据类型来获取数据。
-   kafka模式只支持client模式导入数据。
-   对于边数据，必须指定source和target字段，明确指明这条边对应的两个顶点，不允许isGeo为true的情况。
-   如果数据为null，在生成kafka消息时，直接往json里put空值即可，如jsonObj.put(stringField, null)。

### kafka数据重载
kafka导入失败的ngql语句会落盘到errorpath指定的文件里。我们一共定义了三种重载数据的方式：
-   continue:重载一次保存在error.output下的数据，不管执行成功与否。重载完后会继续消费kafka消息。
-   onlyonce:重载一次保存在error.output下的数据，不管执行成功与否。重载完后退出程序不消费kafka消息。
-   needretry:一直重载保存在error.output下的数据，直到全部重载成功或者达到retry次数限制。若不定义retry，则默认为3。执行完后退出程序不消费kafka消息。

continue模式适合在开启新的程序时，先重载一次数据，然后再消费。onlyonce和needretry模式适合在不影响已经运行的程序的情况下，另开一个程序处理错误语句。  
我们修改了程序里reload数据的逻辑，每次都会只保留本次reload失败的数据，并删除上次reload失败的数据。使用时请再确保重载的数据没有语义和语法错误。  
此外，由于kafka写入错误语句的文件需要复用，因此我们将文件的写入方式改为append。  


## 贡献

Nebula Exchange 2.0 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与：

- 前往 [Nebula Graph 论坛](https://discuss.nebula-graph.com.cn/ "点击前往“Nebula Graph 论坛") 上参与 Issue 讨论，如答疑、提供想法或者报告无法解决的问题
- 撰写或改进文档
- 提交优化代码
