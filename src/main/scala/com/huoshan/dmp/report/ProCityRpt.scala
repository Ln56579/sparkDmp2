package com.huoshan.dmp.report

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Description : 统计日志文件中的省市的数据量  分布情况
  *         将统计出来的结果存到mysql中   并保持一份json格式
  *
  *         本次统计是基于ETL后的数据
  *
  * Created by ln on : 2018/8/19 17:55 
  * Author : ln56
  */
object ProCityRpt {

  def main(args: Array[String]): Unit = {
    //0 检验参数的过滤
    if (args.length!=2) {
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
    val Array(logInputPath , resultOutputPath) = args
    //2 创建sparkConf ---> sparkContext
    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    //RDD的序列化   worker 和 worker直接的通信
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    //创建sql
    val sqlContext = new SQLContext(sc)

    //读取parquet文件
    val df : DataFrame = sqlContext.read.parquet(logInputPath)
    //将  dataFrame  注册成一张临时表
    df.registerTempTable("log")

    val df2 = sqlContext.sql("SELECT provincename,cityname,count(*) counts FROM log GROUP BY provincename,cityname")
    //TODO coalesce(1) 合并小文件
    df2.coalesce(1).write.json(resultOutputPath)

    //判读文件是否存在    删除结果文件
    val hadoopConf = sc.hadoopConfiguration
    val fs = FileSystem.newInstance(hadoopConf)
    if (fs.exists(new Path(resultOutputPath))){
      fs.delete(new Path(resultOutputPath),true)
    }

    sc.stop()

  }


}
