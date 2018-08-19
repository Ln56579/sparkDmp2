package com.huoshan.dmp.tools

import com.huoshan.dmp.util.LogBean
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Description : 安照字段  ---> 进行分区
  * Created by ln on : 2018/8/19 17:39 
  * Author : ln56
  */
object Bzip2Parquet_v2 {
  def main(args: Array[String]): Unit = {
    //0 检验参数的过滤
    if (args.length!=3) {
      println(
        """
          |com.huoshan.dmp.tools
          |参数 :
          |   logInputPath
          |   compressionCode <snappy, gzip, lzo>
          |   resultOutputPath
        """.stripMargin
      )
      sys.exit()
    }
    //1 接受参数
    val Array(logInputPath , compressionCode , resultOutputPath) = args
    //2 创建sparkConf ---> sparkContext
    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    //RDD的序列化   worker 和 worker直接的通信
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //TODO  注册自定义类   使用这个序列化的时候
    sparkConf.registerKryoClasses(Array(classOf[LogBean]))

    val sc = new SparkContext(sparkConf)
    //创建sql
    val sqlContext = new SQLContext(sc)
    //设置输出的压缩格式
    sqlContext.getConf("spark.sql.parquet.compression.codec", compressionCode)

    //日志读取数据
    val logData = sc.textFile(logInputPath)

    val logRdd: RDD[LogBean] = logData.map(line => line.split(",",-1)).filter(_.length>=85).map(arr => LogBean(arr))

    val dataFrame: DataFrame = sqlContext.createDataFrame(logRdd)

    dataFrame.write.partitionBy("provincename","cityname").parquet(resultOutputPath)

    sc.stop()
  }
}
