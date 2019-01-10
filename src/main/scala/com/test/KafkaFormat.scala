package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object KafkaFormat {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("KafkaFormat")
      .getOrCreate()


    // 读取kafka的数据
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


    val res = lines.map { line =>
      val columns = line._2.split(",")
      (columns(0), columns(1), columns(2))
    }.toDF()

    res.createOrReplaceTempView("tmp")

    val result = spark.sql("select _1 as name, _2 as age , _3 as sex from tmp")

    val query = result.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
