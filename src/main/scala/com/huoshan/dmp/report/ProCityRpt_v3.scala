package com.huoshan.dmp.report

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description : TODO
  * Created by ln on : 2018/8/19 20:41 
  * Author : ln56
  */
object ProCityRpt_v3 {
  def main(args: Array[String]): Unit = {
    //0 检验参数的过滤
    if (args.length != 2) {
      println(
        """
          |com.huoshan.dmp.tools
          |参数 :
          |   logInputPath
          |   resultOutputPath
        """.stripMargin
      )
      sys.exit()
    }
    //1 接受参数
    val Array(logInputPath, resultOutputPath) = args
    //2 创建sparkConf ---> sparkContext
    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    //RDD的序列化   worker 和 worker直接的通信
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    //创建sql
    val dataLog = sc.textFile(logInputPath)
    val lineArray = dataLog.map(line => line.split(",",-1)).filter(_.length>=85)
    lineArray.map( arr => {
      ((arr(24),arr(25)),1)
    }).reduceByKey(_+_).map(t => t._1._1+","+t._1._2+","+t._2).saveAsTextFile(resultOutputPath)

    sc.stop()
  }
}
