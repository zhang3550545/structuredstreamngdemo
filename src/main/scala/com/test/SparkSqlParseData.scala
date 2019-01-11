package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Test {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .master("local")
      .appName("Test")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9998)
      .load()

    import spark.implicits._


    lines.createOrReplaceTempView("tmp")

    val res = spark.sql("select split(value,',') as a from tmp")

    res.createOrReplaceTempView("tmp2")

    val res2 = spark.sql("select a[0] as name,a[1] as age, a[2] as sex from tmp2")


    val query = res2.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
