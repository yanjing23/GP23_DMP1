package LOCATION
import UTIL.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**
  * 媒体指标分析
  */
object APP {
  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath,docs) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 读取数据字典
    // 以\\s进行切分(\\s可以切分空格+回车)直到切分完成
    //然后进行过滤,提取出响应字段,存入map形式
    val docMap = spark.sparkContext.textFile(docs).map(_.split("\\s",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
    // 对数据字典进行广播
    val broadcast = spark.sparkContext.broadcast(docMap)
    // 读取数据文件
    val df = spark.read.parquet(inputPath)
    df.rdd.map(row=>{
      // 取媒体相关字段
      var appName = row.getAs[String]("appname")
      //对媒体字段进行判断是否为空
      if(StringUtils.isBlank(appName)){
        //如果为空去广播变量中的的appid 没有取其他
        appName = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //再继续调用RptUtils中的方法

      // 处理请求数
      val rptList = RptUtils.ReqPt(requestmode,processnode)
      // 处理展示点击
      val clickList = RptUtils.clickPt(requestmode,iseffective)
      // 处理广告
      val adList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)
      // 所有指标
      val allList:List[Double] = rptList ++ clickList ++ adList
      (appName,allList)
    }).reduceByKey((list1,list2)=>{
      // list1(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      list1.zip(list2).map(t=>t._1+t._2)
    })
      .map(t=>t._1+","+t._2.mkString(","))

      //      .saveAsTextFile(outputPath)
      .foreach(println)
  }
}
