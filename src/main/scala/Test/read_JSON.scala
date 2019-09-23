package Test

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


import scala.collection.mutable.ListBuffer

object read_JSON {
  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setMaster("local[*]").setAppName("read_JSON")
    val sc = new SparkContext(conf)
    val jsonStr: RDD[String] = sc.textFile("E:\\json.txt")
    val rdd: RDD[String] = jsonStr.map(s => {
      val object1: JSONObject = JSON.parseObject(s)
      val status: Int = object1.getIntValue("status")
      if (status == 0) return
      val object2: JSONObject = object1.getJSONObject("regeocode")
      if (object2 == null) return ""
      val object3: JSONObject = object2.getJSONObject("pois")
      if (object3 == null) return ""
      val jsonarray: JSONArray = object3.getJSONArray("businessarea")
      if (jsonarray == null) return ""
      val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()

      for (item <- jsonarray.toArray()) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          val name = json.getString("name")
          result.append(name)
        }
      }
      result.mkString(",")
    })
    rdd.foreach(println)











  }
}
