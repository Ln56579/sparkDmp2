package com.huoshan.dmp.util
/**
  * Description : String2Int的封装  主要处理 Unit
  * Created by ln on : 2018/8/19 16:00
  * Author : ln56
  */
object NumberFormat {
  // Description : String --[ 转化 ]--> Int
  def toInt (str : String) : Int ={
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }
  }
  // Description : String --[ 转化 ]--> Double
  def toDouble (str :String):Double= {
    try {
      str.toDouble
    } catch {
      case _: Exception => 0
    }
  }

}
