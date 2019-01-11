package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object StructuredWriteMysql {
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

    lines.createOrReplaceTempView("tmp1")

    val lines2 = spark.sql("select split(value,',') as a from tmp1")

    lines2.createOrReplaceTempView("tmp2")

    val result = spark.sql("select a[0] as name, a[1] as age, a[2] as sex,a[3] as uuid from tmp2")

    val mysqlSink = new MysqlSink("jdbc:mysql://localhost:3306/test", "root", "root")

    val query = result.writeStream
      .outputMode("append")
      .foreach(mysqlSink)
      .start()

    query.awaitTermination()
  }
}
