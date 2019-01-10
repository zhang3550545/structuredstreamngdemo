package com.test

import java.io.{FileInputStream, InputStream}
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Props {
  private val prop = new Properties()

  prop.load(getPropertyFileInputStream)

  /**
    * 在spark-submit中加入--driver-java-options -DPropPath=/home/spark/prop.properties的参数后，
    * 使用System.getProperty("PropPath")就能获取路径：/home/spark/prop.properties如果spark-submit中指定了
    * prop.properties文件的路径，那么使用prop.properties中的属性，否则使用该类中定义的属性
    */
  private def getPropertyFileInputStream: InputStream = {
    var is: InputStream = null
    val filePath = System.getProperty("PropPath")
    if (filePath != null && filePath.length > 0) {
      if (Files.exists(Paths.get(filePath))) {
        is = new FileInputStream(filePath)
      } else {
        println(s"在本地未找到config文件$filePath，尝试在HDFS上获取文件")
        val fs = FileSystem.get(new Configuration())
        if (fs.exists(new Path(filePath))) {
          val fis = fs.open(new Path(filePath))
          is = fis.getWrappedStream
        } else {
          println(s"在HDFS上找不到config文件$filePath，加载失败...")
        }
      }
    } else {
      println(s"未设置配置文件PropPath")
    }
    is
  }


  def get(propertyName: String, defaultValue: String): String = {
    prop.getProperty(propertyName, defaultValue)
  }


  def get(): Properties = {
    println("prop:" + this.prop)
    this.prop
  }


  def reload(): Properties = {
    prop.load(getPropertyFileInputStream)
    prop
  }
}
