//package com.tags
//
//import org.apache.commons.lang3.StringUtils
//import org.apache.spark.sql.Row
//
////广告标签(广告类型和广告名称标签)
//
//object tagAd extends tag {
//  override def makeTags(args: Any*): List[(String, Int)] = {
//    val list = List[(String,Int)]()
//    //获取数据
//    val row: Row = args(0).asInstanceOf[Row]
//    //获取广告位类型和广告位名称
//    val adType: Int = row.getAs[Int]("adspacetype")
//    //广告位类型标签
//    adType match {
//      case v if v > 9 => list: +=("LC"+v,1)
//      case v if v > 0 => list: +=("LC0"+v,1)
//    }
//    val adName: String = row.getAs[String]("adspacetypename")
//    //广告位名称标签
//// if (StringUtils.isBlank(adName)){
////      list: += "LN"+adName ->1
////    }
//    list
//  }
//}
