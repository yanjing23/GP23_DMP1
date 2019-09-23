package com.util

import org.apache.spark.sql.Row

object TagsKeywords extends Tagss {
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val stopwords = args(1).asInstanceOf[Map[String,Int]]

    val kws: String = row.getAs[String]("keywords")

    kws.split("\\|")
      .filter(kw=>kw.length >= 3 && kw.length<=8 && !stopwords.contains(kw))
      .foreach(kw=>map += "K" + kw -> 1)

    map
  }
}
