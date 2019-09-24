package LOCATION

import UTIL.RptUtils
import org.apache.spark.sql.SparkSession

/**
  * 统计地域指标
  */
object LoactionRpt {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath,outputPath) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    // 获取数据
    val df = spark.read.parquet(inputPath)
    //把datafream转化为rdd进行操作
    df.rdd.map(row=>{
      // 根据指标的字段获取数据,它取到的数据就是一个row类型的数据
      // REQUESTMODE	PROCESSNODE	ISEFFECTIVE	ISBILLING	ISBID	ISWIN	ADORDERID WinPrice adpayment
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //对取到的数据进行处理,然后一个字段一个字段的计算单独设计为一个方法

      // 1.处理请求数
      val rptList = RptUtils.ReqPt(requestmode,processnode)
      // 2.处理展示点击
      val clickList = RptUtils.clickPt(requestmode,iseffective)
      // 3.处理广告
      val adList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)
      // 4.所有指标----把所有指标进行聚合最终形成一个集合
      val allList:List[Double] = rptList ++ clickList ++ adList
      //最终把((省,市),所有集合)进行返回--以省,市为key形成元组
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)
    }).reduceByKey((list1,list2)=>{
      // list1(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      //对两参数进行拉链操作,如上式,然后进行累加
      list1.zip(list2).map(t=>t._1+t._2)
    })
      //最后把key和值进行以逗号分开  ((省,市),2)
      .map(t=>t._1+","+t._2.mkString(","))

      .saveAsTextFile(outputPath)
  }
}
