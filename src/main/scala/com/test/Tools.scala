package com.test

import com.google.gson.Gson

import scala.collection.mutable


object Tools {

  def main(args: Array[String]): Unit = {
    val tools = new Tools()
    val res = tools.parse("{'name':'caocao','age':'32','sex':'male'}", "com.test.People")
    println(res)
  }

  def parseJson(json: String, className: String): List[String] = {
    val tools = new Tools()
    tools.parse(json, className)
  }

  def list2Tuple7(list: List[String]): (String, String, String, String, String, String, String) = {
    val t = list match {
      case List(a) => (a, "", "", "", "", "", "")
      case List(a, b) => (a, b, "", "", "", "", "")
      case List(a, b, c) => (a, b, c, "", "", "", "")
      case List(a, b, c, d) => (a, b, c, d, "", "", "")
      case List(a, b, c, d, e) => (a, b, c, d, e, "", "")
      case List(a, b, c, d, e, f) => (a, b, c, d, e, f, "")
      case List(a, b, c, d, e, f, g) => (a, b, c, d, e, f, g)
      case _ => ("", "", "", "", "", "", "")
    }
    t
  }
}


class Tools {
  def parse(json: String, className: String): List[String] = {
    val list = mutable.ListBuffer[String]()
    val gson = new Gson()
    val clazz = Class.forName(className)
    val obj = gson.fromJson(json, clazz)
    val aClass = obj.getClass
    val fields = aClass.getDeclaredFields
    fields.foreach { f =>
      val fName = f.getName
      val m = aClass.getDeclaredMethod(fName)
      val value = m.invoke(obj).toString
      list.append(value)
    }
    list.toList
  }
}