package com.huoshan.dmp.tools

import com.huoshan.dmp.util.{NumberFormat, SchemaUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *Description : ETL
  */
object Bzip2Parquet {
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

    val sc = new SparkContext(sparkConf)
    //创建sql
    val sqlContext = new SQLContext(sc)
    //设置输出的压缩格式
    sqlContext.getConf("spark.sql.parquet.compression.codec", compressionCode)

    //日志读取数据
    val logData = sc.textFile(logInputPath)
    //处理数据具体逻辑        进行ETL
    val dataRow: RDD[Row] = logData.map(line => line.split(",", -1)).filter(_.length >= 85)
      .map(arr => {
        Row(
          arr(0),
          NumberFormat.toInt(arr(1)),
          NumberFormat.toInt(arr(2)),
          NumberFormat.toInt(arr(3)),
          NumberFormat.toInt(arr(4)),
          arr(5),
          arr(6),
          NumberFormat.toInt(arr(7)),
          NumberFormat.toInt(arr(8)),
          NumberFormat.toDouble(arr(9)),
          NumberFormat.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          NumberFormat.toInt(arr(17)),
          arr(18),
          arr(19),
          NumberFormat.toInt(arr(20)),
          NumberFormat.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          NumberFormat.toInt(arr(26)),
          arr(27),
          NumberFormat.toInt(arr(28)),
          arr(29),
          NumberFormat.toInt(arr(30)),
          NumberFormat.toInt(arr(31)),
          NumberFormat.toInt(arr(32)),
          arr(33),
          NumberFormat.toInt(arr(34)),
          NumberFormat.toInt(arr(35)),
          NumberFormat.toInt(arr(36)),
          arr(37),
          NumberFormat.toInt(arr(38)),
          NumberFormat.toInt(arr(39)),
          NumberFormat.toDouble(arr(40)),
          NumberFormat.toDouble(arr(41)),
          NumberFormat.toInt(arr(42)),
          arr(43),
          NumberFormat.toDouble(arr(44)),
          NumberFormat.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          NumberFormat.toInt(arr(57)),
          NumberFormat.toDouble(arr(58)),
          NumberFormat.toInt(arr(59)),
          NumberFormat.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          NumberFormat.toInt(arr(73)),
          NumberFormat.toDouble(arr(74)),
          NumberFormat.toDouble(arr(75)),
          NumberFormat.toDouble(arr(76)),
          NumberFormat.toDouble(arr(77)),
          NumberFormat.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          NumberFormat.toInt(arr(84))
        )
      })

    val dataFrame = sqlContext.createDataFrame(dataRow,SchemaUtil.logStructType)
    dataFrame.write.parquet(resultOutputPath)
    //关闭sc
    sc.stop()
  }
}
