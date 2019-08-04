package com.test

import com.google.gson.JsonObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by zhang on 2019/8/3.
  */
object Hdfs2RocketMQStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Hdfs2RocketMQStream")
      .config("hadoop.home.dir", "D:/dsoftinstall/hadoop-2.7.5")
      .getOrCreate()

    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", StringType),
        StructField("sex", StringType)
      )
    )

    val ds = spark.read
      .schema(schema)
      .csv("D:/workspacejava/structuredstreamingdemo/data")

    ds.show()

    import spark.implicits._

    val result = ds.map(row => {
      val name = row.getString(0)
      val age = row.getString(1)
      val sex = row.getString(2)

      val obj = new JsonObject()
      obj.addProperty("name", name)
      obj.addProperty("age", age)
      obj.addProperty("sex", sex)

      val body = obj.toString
      body
    })
      .toDF("body")

    result.show()

    result
      .write
      .format("org.apache.spark.sql.rocketmq.RocketMQSourceProvider")
      .option("nameServer", "localhost:9876")
      .option("topic", "spark-hdfs-rmq")
      .save()

    spark.stop()
  }
}
