package report
import com.util.rpt
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//地域分析指标使用sparkcore
object zhibiao1_sparkcore {
  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("参数不合法")
      sys.exit()
    }
    //1.接收程序参数
    val Array(inputPath,outputPath) = args
    //程序入口
    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    // 进行数据的获取
    val df = spark.read.parquet(inputPath)
    //对取的数据进行操作
    df.rdd.map(row =>{
      //是不是原始请求,有效请求,广告请求
      var reqMode= row.getAs[Int]("requestmode")
      val prcNode=row.getAs[Int]("processnode")
    //参与竞价,竞价成功,List(参与竞价,竞价成功,消费,成本)
      val effive = row.getAs[Int]("iseffective")
      val bill = row.getAs[Int]("isbilling")
      val bid = row.getAs[Int]("isbid")
      val orderId = row.getAs[Int]("adorderid")
      val win = row.getAs[Int]("iswin")
      //广告展示,点击
      val winPrice = row.getAs[Double]("winprice")
      val adPayMent = row.getAs[Double]("adpayment")

      val reqList = rpt.caculateReq(reqMode,prcNode)
      val rtbList = rpt.caculaeRtb(effive,bill,bid,orderId,win,winPrice,adPayMent)
      val showClickList = rpt.caculateShowClick(reqMode,effive)

      //所有指标汇总在一个集合中
      val allList:List[Double] = reqList ++ rtbList ++ showClickList

      //以所有集合为value,以省市为key组成元组进行返回
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)

      //对的的元组进行分组聚合操作
    }).reduceByKey((list1,list2)=>{     //它的参数是两个集合,要想使两个集合进行对应位置关联,则用到拉链操作(zip)
      //// list1(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)






  }
}
