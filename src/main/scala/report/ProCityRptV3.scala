package report

import org.apache.spark.{SparkConf, SparkContext}

object ProCityRptV3 {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("参数不合法")
      sys.exit()
    }
    //1.接收程序参数
    val Array(logInputPath,logOutputPath) = args
    //2.创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName("wenjian").setMaster("local[*]")
      //RDD序列化磁盘
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    //读取数据进行统计
    sc.textFile(logInputPath).map(line=>line.split(",",-1))
        .filter(_.length>=85)
        .map(arr=>((arr(24),arr(25)),1))
        .reduceByKey(_+_)
      .map(t=>t._1._1+","+t._1._2+","+t._2)
      .saveAsTextFile(logOutputPath)






    sc.stop()


  }
}
