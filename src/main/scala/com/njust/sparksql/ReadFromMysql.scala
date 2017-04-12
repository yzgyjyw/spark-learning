package com.njust.sparksql

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/12.
  * 使用sparksql从Mysql中读取数据，并将数据写入到Mysql
  */
object ReadFromMysql {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("urlCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val connectionProps = new Properties()
    connectionProps.put("user","root")
    connectionProps.put("password","yzgylq")
    connectionProps.put("driver","com.mysql.jdbc.Driver")

    val df = sqlContext.read.jdbc("jdbc:mysql://localhost:3306/bigdata","iplocation",Array("id","address","accessCount"),connectionProps)
//    val jdbcDF = sqlContext.read.format("jdbc").
//      options(Map("url" -> "jdbc:mysql://127.0.0.1:3306/bigdata",
//        "driver" -> "com.mysql.jdbc.Driver",
//        "dbtable" -> "iplocation", "user" -> "root",
//        "password" -> "yzgylq")).load()

    df.show()

    //将结果写到MySql中
    df.write.jdbc("jdbc:mysql://localhost:3306/bigdata","iplocation2",connectionProps)

    sc.stop()
  }
}
