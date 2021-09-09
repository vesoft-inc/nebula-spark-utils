/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.fix.nebula

import java.util.regex.Pattern

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
  * @Description NebulaBaseWriter is used for 
  * @author huangzhaolai-jk
  * @Date 2021/8/2 - 19:13 
  * @version 1.0.0
  */
abstract class NebulaBaseWriter extends Serializable {

  val BATCH_INSERT_TEMPLATE = "INSERT %s %s(%s) VALUES %s"
  val INSERT_VALUE_TEMPLATE = "%s: (%s)"
  val INSERT_VALUE_TEMPLATE_WITH_POLICY = "%s(%s): (%s)"
  val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE = "%s->%s: (%s)"
  val ENDPOINT_TEMPLATE = "%s(\"%s\")"
  val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE_WITH_POLICY = "%s->%s: (%s)"
  val EDGE_VALUE_TEMPLATE = "%s->%s@%d: (%s)"
  val EDGE_VALUE_TEMPLATE_WITH_POLICY = "%s->%s@%d: (%s)"

  def write(rdd: DataFrame): Unit

  /**
    * Extra value from the row by field name.
    * When the field is null, we will fill it with default value.
    *
    * @param row   The row value.
    * @param field The field name.
    * @return
    */
  def extraValue(row: Row, field: String): Any = {
    val index = row.schema.fieldIndex(field)
    row.schema.fields(index).dataType match {
      case StringType =>
        if (!row.isNullAt(index)) {
          var str = row.getString(index)
          if ("contact_name".equals(field)) {
            removeSpecialChar(str).mkString("\"", "", "\"")
          } else if (str != null && !str.equals("")) {
            str.replace("\\", "\\\\")
              .replace("\"", "\\\"")
              .replace("'", "\\'")
              .mkString("\"", "", "\"")
          } else {
            "\"\""
          }
        } else {
          "\"\""
        }
      case ShortType =>
        if (!row.isNullAt(index)) {
          row.getShort(index).toString
        } else {
          "0"
        }
      case IntegerType =>
        if (!row.isNullAt(index)) {
          row.getInt(index).toString
        } else {
          "0"
        }
      case LongType =>
        if (!row.isNullAt(index)) {
          row.getLong(index).toString
        } else {
          "0"
        }
      case FloatType =>
        if (!row.isNullAt(index)) {
          row.getFloat(index).toString
        } else {
          "0.0"
        }
      case DoubleType =>
        if (!row.isNullAt(index)) {
          row.getDouble(index).toString
        } else {
          "0.0"
        }
      case _: DecimalType =>
        if (!row.isNullAt(index)) {
          row.getDecimal(index).toString
        } else {
          "0.0"
        }
      case BooleanType =>
        if (!row.isNullAt(index)) {
          row.getBoolean(index).toString
        } else {
          "false"
        }
      case TimestampType =>
        if (!row.isNullAt(index)) {
          row.getTimestamp(index).getTime / 1000L
        } else {
          "0"
        }
      case _: DateType =>
        if (!row.isNullAt(index)) {
          row.getDate(index).toString
        } else {
          "\"\""
        }
      case _: ArrayType =>
        if (!row.isNullAt(index)) {
          row.getSeq(index).mkString("\"[", ",", "]\"")
        } else {
          "\"[]\""
        }
      case _: MapType =>
        if (!row.isNullAt(index)) {
          row.getMap(index).mkString("\"{", ",", "}\"")
        } else {
          "\"{}\""
        }
    }
  }

  val SPECIAL_CHAR_PATTERN: Pattern = Pattern.compile("[\n\t\"\'()<>/\\\\]")

  def removeSpecialChar(value: String): String = SPECIAL_CHAR_PATTERN.matcher(value).replaceAll("")

}
