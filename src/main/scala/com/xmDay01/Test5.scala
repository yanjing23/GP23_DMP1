package com.xmDay01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

//3.2.2	终端设备的指标实现
object Test5 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("参数不合法")
      sys.exit()
    }
    //1.接收程序参数
    val Array(inputPath, outputPath) = args
    //程序入口
    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    // 进行数据的获取
    val df = spark.read.parquet(inputPath)
    //对取的数据进行操作
    val sourceRDD: RDD[(String, List[Double])] = df.rdd.map(row => {
      //是不是原始请求,有效请求,广告请求
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

      var arr: List[Double] = {
        List[Double](if (reqMode == 1 && prcNode >= 1) 1 else 0) ++
          List[Double](if (reqMode == 1 && prcNode >= 2) 1 else 0) ++
          List[Double](if (prcNode == 1 && prcNode == 3) 1 else 0) ++
          List[Double](if (effive == 1 && bill == 1 && bid == 1) 1 else 0) ++
          List[Double](if (effive == 1 && bill == 1 && win == 1 && orderId != 0) 1 else 0) ++
          List[Double](if (reqMode == 2 && effive == 1) 1 else 0) ++
          List[Double](if (reqMode == 3 && effive == 1) 1 else 0) ++
          List[Double](if (effive == 1 && bill == 1 && win == 1) winPrice / 1000.0 else 0) ++
          List[Double](if (effive == 1 && bill == 1 && win == 1) adPayMent / 1000.0 else 0)
      }
      (row.getAs[String]("client"), arr)

    })

    //    sourceRDD.reduceByKey((list1,list2)=>{
    //      list1.zip(list2) .map(t=>t._1+t._2)
    //    }).map(t=>t._1+","+t._2.mkString(",")).saveAsTextFile(outputPath)

    sourceRDD.reduceByKey((list1,list2)=>{
      list1.zip(list2) .map(t=>t._1+t._2)
    }).saveAsTextFile(outputPath)
  }
}
