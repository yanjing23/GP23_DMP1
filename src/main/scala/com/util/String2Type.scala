package com.util

object String2Type {

  //定义一个方法把string类型转化为Int类型

  def toInt(str:String):Int={
    try{
      str.toInt
    }catch{
      case _ :Exception => 0  //用全部的数据进行匹配  要是匹配到错  然后就输出0
    }
  }


  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch{
      case _ :Exception => 0.0 //用全部的数据进行匹配  要是匹配到错  然后就输出0.0
    }
  }

}
