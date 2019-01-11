package com.test

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{ForeachWriter, Row}

class MysqlSink(url: String, user: String, pwd: String) extends ForeachWriter[Row] {

  var conn: Connection = _

  override def open(partitionId: Long, epochId: Long): Boolean = {

    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url, user, pwd)
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
    conn.close()
  }
}
