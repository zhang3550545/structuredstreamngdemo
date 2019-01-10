package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CSVFormat {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CSVFormat")
      .getOrCreate()

    // 定义schema
    val schema = StructType(
      List(
        StructField("name", StringType),
        StructField("age", StringType),
        StructField("sex", StringType)
      )
    )

    val lines = spark.readStream
      .format("csv")
      .schema(schema)
      .load("/Users/zhangzhiqiang/Documents/my_projects/structuredstreamngdemo/data")

    val query = lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
