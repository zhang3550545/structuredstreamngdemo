package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .master("local")
      .appName("WordCount")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9998")
      .load()

    // 隐式转换
    import spark.implicits._

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }
}
