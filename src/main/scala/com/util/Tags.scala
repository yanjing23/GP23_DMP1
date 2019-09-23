//package com.util
//
//import org.apache.spark.sql.Row
//
//import scala.collection.mutable.ListBuffer
//
//object Tags {
//
//  val hasSomeUserIdConditition = """
//                              | imei != "" or imeimd5 != "" or imeisha1 != "" or
//                              | idfa != "" or idfamd5 != "" or idfasha1 != "" or
//                              | mac != "" or macmd5 != "" or macsha1 != "" or
//                              | androidid != "" or androididmd5 != "" or androididsha1 != "" or
//                              | openudid != "" or openudidmd5 != "" or openudidsha1 != ""
//                             """.stripMargin
//
//
//
//
//
////提取用户的所有的ID
//  def getAllUser(row: Row):ListBuffer[String]={
//    val userIds = new collection.mutable.ListBuffer[String]()
//
//    row match {
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//      case v if v.getAs[String]("idfa")nonEmpty => userIds.append("DF:"+v.getAs[String]("idfa").toUpperCase)
//      case v if v.getAs[String]("mac")nonEmpty => userIds.append("MAC:"+v.getAs[String]("mac").toUpperCase)
//      case v if v.getAs[String]("androidid")nonEmpty => userIds.append("AD:"+v.getAs[String]("androidid").toUpperCase)
//      case v if v.getAs[String]("openudid")nonEmpty => userIds.append("OP:"+v.getAs[String]("openudid").toUpperCase)
//
//
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//
//
//
//
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//      case v if v.getAs[String]("imei")nonEmpty => userIds.append("IM:"+v.getAs[String]("imei").toUpperCase)
//
//
//
//    }
//
//  }
//
//}
