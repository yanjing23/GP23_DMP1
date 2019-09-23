package com.Location

import com.util.rpt
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession

//媒体指标分析
object APP {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("参数不合法")
      sys.exit()
    }
    //1.接收程序参数    doc代表的是数据字典
    val Array(inputPath, outputPath,doc) = args
    //程序入口
    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //1.读取数据字典
    val docMap: collection.Map[String, String] = spark.sparkContext.textFile(doc)
      //对读到的数据字典进行规则进行切分和过滤
      //切分规则--以\\s---切分所有的空格加tab键
      //过滤的是它最少5个字段
      .map(_.split("\\s", -1)).filter(_.length >= 5)
      //对过滤后的数据取第2个字段和第二个字段
      //然后进行收集再转化成map
      .map(arr => (arr(4), arr(1))).collectAsMap()

    //对上述的取到的数据进行广播
    val broadcast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docMap)

    //读取数据文件(拿到parquet文件然后和拿到的数据字典文件进行匹配)
    val df: DataFrame = spark.read.parquet(inputPath)

    df.rdd.map(row=>{

      //提取媒体相关字段
      val appName = row.getAs[String]("appname")
      //对appName进行判断--要是appName为空则进行取广播中的appid ,否则为其他
      if(StringUtils.isBlank(appName)){
       broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }

      var reqMode = row.getAs[Int]("requestmode")
      val prcNode = row.getAs[Int]("processnode")
      //参与竞价,竞价成功,List(参与竞价,竞价成功,消费,成本)
      val effive = row.getAs[Int]("iseffective")
      val bill = row.getAs[Int]("isbilling")
      val bid = row.getAs[Int]("isbid")
      val orderId = row.getAs[Int]("adorderid")
      val win = row.getAs[Int]("iswin")
      //广告展示,点击
      val winPrice = row.getAs[Double]("winprice")
      val adPayMent = row.getAs[Double]("adpayment")

      //调用方法进行实现,然后再传入参数
      val reqList = rpt.caculateReq(reqMode,prcNode)
      val rtbList = rpt.caculaeRtb(effive,bill,bid,orderId,win,winPrice,adPayMent)
      val showClickList = rpt.caculateShowClick(reqMode,effive)

      //汇集所有集合为一个总集合
      val allList:List[Double] = reqList ++ rtbList ++ showClickList

      //返回取值(数据字典,又有集合)
      (appName,allList)

      //进行聚合操作,参数是两个集合
    }).reduceByKey((list1,list2)=>{
      //然后对集合进行拉链操作---list((1,1,),(1,1),(1,1))
      //进行聚合操作
      list1.zip(list2).map(t=>t._1+t._2)
    }).saveAsTextFile(outputPath) //然后进行保存

  }
}
