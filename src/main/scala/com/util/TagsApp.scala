package com.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object TagsApp extends Tagss {
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]

    val appDict: Map[String, Int] = args(1).asInstanceOf[Map[String,Int]]

    val appId: String = row.getAs[String]("appid")
    val appName: String = row.getAs[String]("appname")

    //进行逻辑判断
    if(StringUtils.isNotEmpty(appName)){
      appDict.contains(appId)match{
        case true => map += "APP"+appDict.get(appId) -> 1
      }
    }


    map

  }
}
