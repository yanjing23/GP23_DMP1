package UTIL

/**
  * 类型工具类
  */
//定义一个方法是String类型转Int类型
object String2Type {

  def toInt(str:String):Int={
    try{
      str.toInt
    }catch {
      case _ :Exception =>0
    }
  }

  //定义一个方法String 类型转Double类型

  def toDouble(str: String):Double ={
    try{
      str.toDouble
    }catch {
      case _ :Exception =>0.0
    }
  }
}

