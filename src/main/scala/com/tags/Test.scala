package com.tags

import org.apache.spark.sql.SparkSession

//测试类
object Test {

  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
//    //val arr = Array("https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=9ee7a1af927477bfd38944e39e99f0d1")
//
//      val arr = Array("https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=d974d18aad003b47fa765ae6224020d4")
//                                                                                                                      //	9ee7a1af927477bfd38944e39e99f0d1
//    val rdd = spark.sparkContext.makeRDD(arr)
//    rdd.map(t=>{
//      HttpUtil.get(t)
//    })
//      .foreach(println)

    val url="https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=d974d18aad003b47fa765ae6224020d4"
    val res: String = HttpUtil.get(url)
    println(res)


  }
}
