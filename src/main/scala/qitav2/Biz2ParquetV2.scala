package qitav2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

//使用自定义类的方式构建scheame信息
object Biz2ParquetV2 {
  def main(args: Array[String]): Unit = {
    //0.校验参数个数
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

    val sQLContext = new SQLContext(sc)

    //设置压缩方式
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")

    //3.读取日志文件
    sc.textFile(logInputPath).map(line=>line.split(",",-1)).filter(_.length>=85)

  }

}
