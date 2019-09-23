package qita

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

//将原始的日志文件转换为parquet文件格式
object Bzip2Parquet {
  def main(args: Array[String]): Unit = {
    //0.校验参数个数
    if(args.length != 3){
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

    //3.读取日志文件
    val rowdata = sc.textFile(logInputPath)

    val sQLContext = new SQLContext(sc)

    //设置压缩方式
    sQLContext.setConf("spark.sql.parquet.compression.codec","compressionCode")

    //4.根据业务需求对数据进行ETL
    rowdata.map(t=>t.split(",",-1)).filter(_.length>=85)




    //5.将结果存储在本地磁盘
    //6.关闭sc
  }
}
