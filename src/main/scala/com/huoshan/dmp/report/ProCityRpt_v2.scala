package com.huoshan.dmp.report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Description : TODO
  * Created by ln on : 2018/8/19 19:19 
  * Author : ln56
  */
object ProCityRpt_v2 {

  def main(args: Array[String]): Unit = {
    //0 检验参数的过滤
    if (args.length!=1) {
      println(
        """
          |com.huoshan.dmp.tools
          |参数 :
          |   logInputPath
        """.stripMargin
      )
      sys.exit()
    }
    //1 接受参数
    val Array(logInputPath ) = args
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
    //加载配置文件 application.conf -> application.json --> application.properties
    val load = ConfigFactory.load()
    val tableName = load.getString("jdbc.tableName")
    val url = load.getString("jdbc.url")
    val user = load.getString("jdbc.user")
    val password = load.getString("jdbc.password")
    val properties = new Properties()
    properties.setProperty("user",user)
    properties.setProperty("password",password)
    //把数据写入到mysql中
    df2.write.mode(SaveMode.Overwrite).jdbc(url,tableName,properties)


    sc.stop()

  }

}
