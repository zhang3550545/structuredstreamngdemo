package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CommonStructuedKafka {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val masterUrl = Props.get("master", "local")
    val appName = Props.get("appName", "Test7")
    val className = Props.get("className", "")
    val kafkaBootstrapServers = Props.get("kafka.bootstrap.servers", "localhost:9092")
    val subscribe = Props.get("subscribe", "test")
    val tmpTable = Props.get("tmpTable", "tmp")
    val sparksql = Props.get("sparksql", "select * from tmp")


    val spark = SparkSession.builder()
      .master(masterUrl)
      .appName(appName)
      .getOrCreate()


    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", subscribe)
      .load()

    //隐式转换
    import spark.implicits._

    val values = lines.selectExpr("cast(value as string)").as[String]

    val res = values.map { value =>
      // 将json数据解析成list集合
      val list = Tools.parseJson(value, className)
      // 将List转成元组
      Tools.list2Tuple7(list)
    }

    res.createOrReplaceTempView(tmpTable)

    val result = spark.sql(sparksql)

    val query = result.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
