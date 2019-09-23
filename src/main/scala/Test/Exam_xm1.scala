package Test

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Exam_xm1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Exam_xm1")
    val sc = new SparkContext(conf)
    //读取数据
    val str: RDD[String] = sc.textFile("E:\\json.txt")
    //解析json数据
    val jsonRDD = str.flatMap(s => {
      val list: ListBuffer[String] = collection.mutable.ListBuffer[String]()
      val strJSONObject: JSONObject = JSON.parseObject(s)
      val status: Int = strJSONObject.getIntValue("status")
      if (status == 0) ""
      else {
        val Object1: JSONObject = strJSONObject.getJSONObject("regeocode")
        if (Object1 == null) ""
        else {
          val array1: JSONArray = Object1.getJSONArray("pois")
          if (array1 == null) ""
          else {
            for (ele <- array1.toArray()) {
              val elebject2: JSONObject = ele.asInstanceOf[JSONObject]
              val elebusinessarea: String = elebject2.getString("businessarea")
              val eletype: String = elebject2.getString("type")
              val ele1 = elebusinessarea + "," + eletype
              list.append(ele1)
            }
            list
          }
          list
        }
        list
      }
      list
    })
    // 1、按照pois，分类businessarea，并统计每个businessarea的总数。

    jsonRDD.map(t => {
      val arr: Array[String] = t.split(",")
      val str: String = arr(0)
      (str, 1)
    }).reduceByKey(_ + _).foreach(println)

    val typerdd = jsonRDD.flatMap(t => {
      val str: Array[String] = t.split(",")
      val types: String = str(1)
      types.split(";")
    })

    val typetupe1: RDD[(String, Int)] = typerdd.map(tp => (tp, 1))

    val resource  = typetupe1.reduceByKey(_+_)

    resource.collect().toBuffer.foreach(println)









  }

}
