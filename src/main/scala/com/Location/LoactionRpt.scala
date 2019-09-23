//package com.Location
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//
///**
//  * 实现地域指标
//  */
//object LoactionRpt {
//  def main(args: Array[String]): Unit = {
//
//    if(args.length != 2){
//      println("目录不正确,退出程序")
//      sys.exit()
//    }
//    //获取目录参数
//    val Array(inputPath,outputPath) = args
//
//    val spark= SparkSession.setAppName("this.getClass.getName").setMaster("local[*]")
//
//      //设置序列化级别
//      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//
//    val df = spark.read
//
//  }
//}
