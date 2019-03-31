package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Structured Streaming exactly once
  *
  *  1.重复数据的处理
  *
  * 可以通过对DataSet数据集进行dropDuplicates()处理，也可以通过指定key来处理。
  * dropDuplicates()：指一条数据完全一样，就drop
  * dropDuplicates(columns)：多个列一样，就drop
  *
  *  2.Streaming的程序异常处理
  *
  * 通过checkpoint指定目录，最好是HDFS可靠的分布式文件系统
  * 保障Streaming异常后的恢复处理
  * checkpoint是可以保证至少一次的语义的
  *
  *
  */

object StructuredStreamingExactlyOnce {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StructuredStreamingExactlyOnce")
      .getOrCreate()

    // 读取kafka流数据
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()


    // 隐式转换
    import spark.implicits._

    // 截取 key，value 2个字段
    val lines = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]


    val query = lines.dropDuplicates().writeStream
      .format("console")
      .option("checkpointLocation", "/Users/zhangzhiqiang/Documents/my_projects/structuredstreamngdemo/checkpoint")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
