package report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 地域分析指标
  */
object zhibiao1 {
  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      println("参数不合法")
      sys.exit()
    }
    //1.接收程序参数
    val Array(logInputPath) = args
    //2.创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName("wenjian").setMaster("local[*]")
      //RDD序列化磁盘
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    //统计地域分析指标
    val sQLContext = new SQLContext(sc)

    //读取parquet文件
      val parquetData: DataFrame = sQLContext.read.parquet(logInputPath)

    parquetData.registerTempTable("log")

//    val frame: DataFrame = sQLContext.sql(
//      """
//select
//provincename,cityname,
//sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) 有效请求,
//sum(case when requestmode=1 and processnode =3 then 1 else 0 end)广告请求,
//sum(case when iseffective=1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)参与竞价数,
//sum(case when iseffective=1 and isbilling =1 and iswin =1 then 1 else 0 end)竞价成功数,
//sum(case when requestmode=2 and iseffective =1 then 1 else 0 end)展示数,
//sum(case when requestmode=3 and iseffective =1 then 1 else 0 end)点击数,
//sum(case when iseffective=1 and isbilling =1 and iswin =1 then 1.0*adpayment/1000 else 0 end)广告成本,
//sum(case when iseffective=1 and isbilling =1 and iswin =1 then 1.0*winprice/1000 else 0 end) 广告消费
//from log
//group by provincename,cityname
//      """.stripMargin)
//    frame.show()


    //逻辑处理
    val result = sQLContext.sql(
      """
|select
|provincename,cityname,
|sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) 有效请求,
|sum(case when requestmode=1 and processnode =3 then 1 else 0 end)广告请求,
|sum(case when iseffective=1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)参与竞价数,
|sum(case when iseffective=1 and isbilling =1 and iswin =1 then 1 else 0 end)竞价成功数,
|sum(case when requestmode=2 and iseffective =1 then 1 else 0 end)展示数,
|sum(case when requestmode=3 and iseffective =1 then 1 else 0 end)点击数,
|sum(case when iseffective=1 and isbilling =1 and iswin =1 then 1.0*adpayment/1000 else 0 end)广告成本,
|sum(case when iseffective=1 and isbilling =1 and iswin =1 then 1.0*winprice/1000 else 0 end) 广告消费
|from log
|group by provincename,cityname

     """ .stripMargin
    )
    //result.show()

    val load = ConfigFactory.load()
    //创建Properties对象
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))

    //存储
    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),
      load.getString("jdbc.arearpt.table"),prop)

    sc.stop()
  }
}
