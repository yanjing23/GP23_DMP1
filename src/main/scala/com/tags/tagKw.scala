//package com.tags
//
//import org.apache.spark.sql.Row
//
//object tagKw extends tag {
//  override def makeTags(args: Any*): Map[(String, Int)] = {
//
//    var map = Map[String,Int]()
//    val row: Row = args(0).asInstanceOf[Row]
//    val stopwords = args(1).asInstanceOf[Map[String,Int]]
//
//    val kws: String = row.getAs[String]("keywords")
//
//    kws.split("\\|")
//      .filter(kw=>kw.length >= 3 && kw.length<=8 && !stopwords.contains(kw))
//      .foreach(kw=>map += "K" + kw -> 1)
//
//    map
//  }
//
//}
