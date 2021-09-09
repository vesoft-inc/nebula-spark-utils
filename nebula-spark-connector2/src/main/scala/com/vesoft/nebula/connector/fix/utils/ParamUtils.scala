package com.qihoo.finance.graph.data.utils


/**
  * @Description ParamUtils is used for 
  * @author huangzhaolai-jk
  * @Date 2021/7/30 - 10:51
  * @version 1.0.0
  */
object ParamUtils {

  def getConfig[T](t: T, defaultValue: T): T = {
    if (t == null) {
      return defaultValue
    }
    t
  }

}
