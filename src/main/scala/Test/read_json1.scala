package Test

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.StringOps
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object read_json1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("read_json").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val jsonStr: RDD[String] = sc.textFile("E:\\json.txt")
    //对接收到的json文件进行解析
    val rdd = jsonStr.flatMap(t => {
      val jsonObject: JSONObject = JSON.parseObject(t)
      val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
      val status: Int = jsonObject.getIntValue("status")
      if (status == 0) {}
      else {
        val regeocode: JSONObject = jsonObject.getJSONObject("regeocode")
        if (regeocode == null || regeocode.keySet().isEmpty()) {}
        else {
          val poisArray: JSONArray = regeocode.getJSONArray("pois")
          if (poisArray.isEmpty || poisArray == null) {}
          else {
            for (ele <- poisArray.toArray()) {
              val eleJsonObject: JSONObject = ele.asInstanceOf[JSONObject]
              val businessarea: String = eleJsonObject.getString("businessarea")
              val tp: String = eleJsonObject.getString("type")

              val info = businessarea + "," + tp
              result.append(info)
            }
            result
          }
          result
        }
        result
      }
      result
    })
    val businessTup = rdd.map(str => {
      val group: Array[String] = str.split(",")
      val businessArea: String = group(0)
      (businessArea, 1)
    })
    val typeRdd: RDD[String] = rdd.flatMap(str => {
      val group: mutable.ArrayOps[String] = str.split(",")
      val types: String = group(1)
      types.split(";")
    })
    val typeTup: RDD[(String, Int)] = typeRdd.map(tp => (tp, 1))

    println("---------------------第一个标签----------------------")

    val businessRes: RDD[(String, Int)] = businessTup.reduceByKey(_ + _)

    businessRes.collect().toBuffer.foreach(println)

    println("-------------------TYPE-------------------")

    val typeRes: RDD[(String, Int)] = typeTup.reduceByKey(_ + _)

    typeRes.collect().toBuffer.foreach(println)

    sc.stop()

  }
}
