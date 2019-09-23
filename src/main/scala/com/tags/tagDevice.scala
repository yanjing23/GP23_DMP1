//package com.tags
//
//import org.apache.commons.lang3.StringUtils
//import org.apache.spark.sql.Row
//
//object tagDevice extends tag {
//  override def makeTags(args: Any*): List[(String, Int)] = {
//    val list = List[(String,Int)]()
//    val row: Row = args(0).asInstanceOf[Row]
//    val Client: Int = row.getAs[Int]("client")
//    val Networkname: String = row.getAs[String]("networkmannername")
//    val NetworkId: Int = row.getAs[Int]("networkmannerid")
//    val Ispname: String = row.getAs[String]("ispname")
//    val IspId: Int = row.getAs[Int]("ispid")
//    val phoneType = row.getAs[String]("device")
//
//    Client match {
//      case 1 => list: +="D00010001" -> 1
//      case 2 => list: +="D00010002" -> 1
//      case 3 => list: +="D00010003" -> 1
//      case _ => list: +="D00010004" -> 1
//    }
//    if (StringUtils.isBlank(phoneType)) list: += "DN"+phoneType ->1
//
//    Networkname.toUpperCase match {
//      case "WIFI" => list: +="D00020001" -> 1
//      case "4G" => list: +="D00020002" -> 1
//      case "3G" => list: +="D00020003" -> 1
//      case "2G" => list: +="D00020004" -> 1
//      case _ => list: +="D00020005" -> 1
//    }
//
//    Ispname match {
//      case "移动"=> list: += "D00030001" -> 1
//      case "联通"=> list: += "D00030002" -> 1
//      case "电信"=> list: += "D00030003" -> 1
//      case _ => list: += "D00030004" -> 1
//
//
//    }
//
//
//
//
//
//
//
//
//      list
//  }
//}
