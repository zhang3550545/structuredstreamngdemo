package com.test

import java.sql.Connection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}


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


    val query = result.writeStream
      .foreach(new ForeachWriter[Row] {

        var conn: Connection = ConnectionPool.getConnection.get

        override def open(partitionId: Long, epochId: Long): Boolean = {
          true
        }

        override def process(value: Row): Unit = {
          val p = conn.prepareStatement("replace into people(name,age,sex,uuid) values(?,?,?,?)")
          p.setString(1, value(0).toString)
          p.setString(2, value(1).toString)
          p.setString(3, value(2).toString)
          p.setString(4, value(3).toString)
          p.execute()
        }

        override def close(errorOrNull: Throwable): Unit = {
        }

        conn.close()
      }).start()

    query.awaitTermination()
  }
}
