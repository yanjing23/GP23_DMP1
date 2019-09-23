package report

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

//需求1:将统计出来的结果存储成json文件格式
//需求2:将统计出来的结果存储到mysql中
//本次统计是基于parquet文件
object city {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("参数不合法")
      sys.exit()
    }
    //1.接收程序参数
    val Array(logInputPath,resultOutputPath) = args
    //2.创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName("wenjian").setMaster("local[*]")
      //RDD序列化磁盘
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    val sqlc = new SQLContext(sc)


    val df: DataFrame = sqlc.read.parquet(logInputPath)

    df.registerTempTable("log")

    val df2: DataFrame = sqlc.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    df2.write.json(resultOutputPath)






  }
}
