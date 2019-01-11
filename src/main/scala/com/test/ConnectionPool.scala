package com.test

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}

/**
  * 数据库连接池，使用了BoneCP
  */
object ConnectionPool {

  //连接池配置
  private val connectionPool: Option[BoneCP] = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:mysql://localhost/test")
      config.setUsername("root")
      config.setPassword("root")
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)
      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        println("Create Connection Error: \n" + exception.printStackTrace())
        None
    }
  }

  // 获取数据库连接
  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(pool) => Some(pool.getConnection)
      case None => None
    }
  }

  // 释放数据库连接
  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }
}
