package com.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object Tagsads extends Tagss {
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {

    //定义一个空的MAP  记住一定是var--可进行增加的
    var map = Map[String,Int]()
    //调用的时候传递的参数是row类型,在这拿到一行数据之后要进行解析
    val row: Row = args(0).asInstanceOf[Row]

    //然后进行打标签--取广告位类型和名称
    val addTypeId: Int = row.getAs[Int]("adspacetype")
    val addTypeName: String = row.getAs[String]("adspacetypename")

    if(addTypeId > 9) map += "LC"+addTypeId -> 1
    else if (addTypeId > 0) map += "LC0"+addTypeId -> 1
    if(StringUtils.isNotEmpty(addTypeName))map += "LN"+addTypeName -> 1

    //渠道的
    val chanelId: Int = row.getAs[Int]("adplatformproviderid")
    if(chanelId > 0) map += (("CN"+chanelId,1))

    map
  }
}
