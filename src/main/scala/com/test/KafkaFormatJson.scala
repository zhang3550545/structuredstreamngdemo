package com.test

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object KafkaFormatJson {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .master("local")
      .appName("KafkaFormatJson")
      .getOrCreate()

    // 读取kafka流数据
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    // 隐式转换
    import spark.implicits._

    val values = lines.selectExpr("CAST(value AS STRING)").as[String]

    val res = values.map { value =>
      // 解析json逻辑
      val gson = new Gson
      val people = gson.fromJson(value, classOf[People])
      (people.name, people.age, people.sex)
    }

    res.createOrReplaceTempView("tmp")

    // spark sql
    val result = spark.sql("select _1 as name, _2 as age, _3 as sex from tmp")

    // 写入
    val query = result.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
