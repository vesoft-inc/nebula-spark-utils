/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package scala.com.vesoft.nebula.exchange.processor

import com.vesoft.nebula.exchange.processor.Processor
import com.vesoft.nebula.{Date, DateTime, NullType, Time, Value}
import com.vesoft.nebula.meta.PropertyType
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  IntegerType,
  LongType,
  ShortType,
  StringType,
  StructField,
  StructType
}
import org.junit.Test
import com.alibaba.fastjson.{JSON, JSONObject}
import com.vesoft.nebula.exchange.utils.NebulaUtils.DEFAULT_EMPTY_VALUE

class ProcessorSuite extends Processor {
  val values = List("Bob",
                    "fixedBob",
                    12,
                    200,
                    1000,
                    100000,
                    "2021-01-01",
                    "2021-01-01T12:00:00",
                    "12:00:00",
                    "2021-01-01T12:00:00",
                    true,
                    12.01,
                    22.12,
                    null)
  val schema: StructType = StructType(
    List(
      StructField("col1", StringType, nullable = true),
      StructField("col2", StringType, nullable = true),
      StructField("col3", ShortType, nullable = true),
      StructField("col4", ShortType, nullable = true),
      StructField("col5", IntegerType, nullable = true),
      StructField("col6", LongType, nullable = true),
      StructField("col7", StringType, nullable = true),
      StructField("col8", StringType, nullable = true),
      StructField("col9", StringType, nullable = true),
      StructField("col10", StringType, nullable = true),
      StructField("col11", BooleanType, nullable = true),
      StructField("col12", DoubleType, nullable = true),
      StructField("col13", DoubleType, nullable = true),
      StructField("col14", StringType, nullable = true)
    ))
  val row = new GenericRowWithSchema(values.toArray, schema)
  val map = Map(
    "col1"  -> PropertyType.STRING.getValue,
    "col2"  -> PropertyType.FIXED_STRING.getValue,
    "col3"  -> PropertyType.INT8.getValue,
    "col4"  -> PropertyType.INT16.getValue,
    "col5"  -> PropertyType.INT32.getValue,
    "col6"  -> PropertyType.INT64.getValue,
    "col7"  -> PropertyType.DATE.getValue,
    "col8"  -> PropertyType.DATETIME.getValue,
    "col9"  -> PropertyType.TIME.getValue,
    "col10" -> PropertyType.TIMESTAMP.getValue,
    "col11" -> PropertyType.BOOL.getValue,
    "col12" -> PropertyType.DOUBLE.getValue,
    "col13" -> PropertyType.FLOAT.getValue,
    "col14" -> PropertyType.STRING.getValue
  )

  @Test
  def extraValueForClientSuite(): Unit = {
    assert(extraValueForClient(row, "col1", map).toString.equals("\"Bob\""))
    assert(extraValueForClient(row, "col2", map).toString.equals("\"fixedBob\""))
    assert(extraValueForClient(row, "col3", map).toString.toInt == 12)
    assert(extraValueForClient(row, "col4", map).toString.toInt == 200)
    assert(extraValueForClient(row, "col5", map).toString.toInt == 1000)
    assert(extraValueForClient(row, "col6", map).toString.toLong == 100000)
    assert(extraValueForClient(row, "col7", map).toString.equals("date(\"2021-01-01\")"))
    assert(
      extraValueForClient(row, "col8", map).toString.equals("datetime(\"2021-01-01T12:00:00\")"))
    assert(extraValueForClient(row, "col9", map).toString.equals("time(\"12:00:00\")"))
    assert(
      extraValueForClient(row, "col10", map).toString.equals("timestamp(\"2021-01-01T12:00:00\")"))
    assert(extraValueForClient(row, "col11", map).toString.toBoolean)
    assert(extraValueForClient(row, "col12", map).toString.toDouble > 12.00)
    assert(extraValueForClient(row, "col13", map).toString.toDouble > 22.10)
    assert(extraValueForClient(row, "col14", map) == null)
  }

  @Test
  def extraValueForClientFromJSONSuite(): Unit = {
    var jsonObj = JSON.parseObject("{}")
    jsonObj.put("col1", "Bob")
    jsonObj.put("col2", "fixedBob")
    jsonObj.put("col3", 12)
    jsonObj.put("col4", 200)
    jsonObj.put("col5", 1000)
    jsonObj.put("col6", 100000)
    jsonObj.put("col7", "2021-01-01")
    jsonObj.put("col8", "2021-01-01T12:00:00")
    jsonObj.put("col9", "12:00:00")
    jsonObj.put("col10", "2021-01-01T12:00:00")
    jsonObj.put("col11", true)
    jsonObj.put("col12", 12.01)
    jsonObj.put("col13", 22.12)
    jsonObj.put("col14", null)

    assert(extraValueForClientFromJSON(jsonObj, "col1", map).toString.equals("\"Bob\""))
    assert(extraValueForClientFromJSON(jsonObj, "col2", map).toString.equals("\"fixedBob\""))
    assert(extraValueForClientFromJSON(jsonObj, "col3", map).toString.toInt == 12)
    assert(extraValueForClientFromJSON(jsonObj, "col4", map).toString.toInt == 200)
    assert(extraValueForClientFromJSON(jsonObj, "col5", map).toString.toInt == 1000)
    assert(extraValueForClientFromJSON(jsonObj, "col6", map).toString.toLong == 100000)
    assert(extraValueForClientFromJSON(jsonObj, "col7", map).toString.equals("date(\"2021-01-01\")"))
    assert(
      extraValueForClientFromJSON(jsonObj, "col8", map).toString.equals("datetime(\"2021-01-01T12:00:00\")"))
    assert(extraValueForClientFromJSON(jsonObj, "col9", map).toString.equals("time(\"12:00:00\")"))
    assert(
      extraValueForClientFromJSON(jsonObj, "col10", map).toString.equals("timestamp(\"2021-01-01T12:00:00\")"))
    assert(extraValueForClientFromJSON(jsonObj, "col11", map).toString.toBoolean)
    assert(extraValueForClientFromJSON(jsonObj, "col12", map).toString.toDouble > 12.00)
    assert(extraValueForClientFromJSON(jsonObj, "col13", map).toString.toDouble > 22.10)
    assert(extraValueForClientFromJSON(jsonObj, "col14", map) == null)
  }

  @Test
  def extraValueForSSTSuite(): Unit = {
    assert(extraValueForSST(row, "col1", map).toString.equals("Bob"))
    assert(extraValueForSST(row, "col2", map).toString.equals("fixedBob"))
    assert(extraValueForSST(row, "col3", map).toString.toInt == 12)
    assert(extraValueForSST(row, "col4", map).toString.toInt == 200)
    assert(extraValueForSST(row, "col5", map).toString.toInt == 1000)
    assert(extraValueForSST(row, "col6", map).toString.toLong == 100000)
    val date = new Date(2021, 1, 1)
    assert(extraValueForSST(row, "col7", map).equals(date))
    val datetime = new DateTime(2021, 1, 1, 12, 0, 0, 0)
    assert(extraValueForSST(row, "col8", map).equals(datetime))

    val time = new Time(12, 0, 0, 0)
    assert(extraValueForSST(row, "col9", map).equals(time))

    try {
      extraValueForSST(row, "col10", map).toString
    } catch {
      case e: Exception => assert(true)
    }

    assert(extraValueForSST(row, "col11", map).toString.toBoolean)
    assert(extraValueForSST(row, "col12", map).toString.toDouble > 12.0)
    assert(extraValueForSST(row, "col13", map).toString.toFloat > 22.10)

    val nullValue = new Value()
    nullValue.setNVal(NullType.__NULL__)
    assert(extraValueForSST(row, "col14", map).equals(nullValue))
  }

  /**
    * process dataframe to vertices or edges
    */
  override def process(): Unit = ???

}
