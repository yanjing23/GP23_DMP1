//package com.tags
//
//import org.apache.spark.sql.Row
//
//object tagChanel extends tag {
//  override def makeTags(args: Any*): List[(String, Int)] = {
//    val list = List[(String,Int)]()
//    val row: Row = args(0).asInstanceOf[Row]
//    val chanelId: Int = row.getAs[Int]("adplatformproviderid")
////    if(chanelId>0){
////      list: += "CN"+chanelId ->1
////    }
//   chanelId match {
//     case t > 0 =>list: +="CN"+t ->1
//   }
//    list
//  }
//}
