//package com.tags
//
//import org.apache.commons.lang3.StringUtils
//import org.apache.spark.sql.Row
//
////对APP名称进行打标签
//object tagApp extends tag {
//  override def makeTags(args: Any*): List[(String, Int)] = {
//    val list = List[(String,Int)]()
//    //把传进来的数据转化为row类型形式,然后进行处理
//    val row: Row = args(0).asInstanceOf[Row]
//    val AppDoc: List[(String, Int)] = args(1).asInstanceOf[List[(String,Int)]]
//    //提取App名称字段
//    val AppName: String = row.getAs[String]("appname")
//    val AppId: String = row.getAs[String]("appid")
//    if(StringUtils.isBlank(AppName)){
//      AppDoc.contains(AppId) match {
//        case true => list: += "APP" + AppId -> 1
//      }
//    }
//    list
//  }
//}
