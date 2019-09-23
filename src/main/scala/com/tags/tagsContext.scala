//package com.tags
//
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
////标签上下文主类
//object tagsContext {
//  def main(args: Array[String]): Unit = {
//
//    if (args.length != 4){
//      println("目录不正确")
//      sys.exit()
//    }
//    val Array(inputPath,doc,stopwords,outputPath) = args
//    //创建spark上下文
//    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("主类").getOrCreate()
//    //读取数据parquet文件
//    val df: DataFrame = spark.read.parquet(inputPath)
//
//    //字典文件进行读取
//    val docMap: collection.Map[String, String] = spark.sparkContext.textFile(doc).map(line => {
//      val fields: Array[String] = line.split("\\s", -1)
//      (fields(4), fields(1))
//    }).collectAsMap()
//
//    //对敏感字进行读取
//        val stopwordsMap: collection.Map[String, Int] = spark.sparkContext.textFile(stopwords).map((_,0)).collectAsMap()
//
//    //对字典文件和敏感字文件进行广播
//    val docBC: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docMap)
//    val stopwordsBC: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(stopwordsMap)
//
//
//    //处理数据信息
//    df.map(row=>{
//      //进行取值--先获取用户的Id(设计一个方法)
//    val userId: String = tagUtils.getOneUserId(row)
//      //标签实现(封装成方法进行调用)
//
//
//    })
//
//  }
//}
