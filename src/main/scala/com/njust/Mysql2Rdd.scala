package com.njust

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/11.
  *从mysql中获取数据
  * 1.创建一个JdbcConnect
  */
object Mysql2Rdd {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //getConnection: () => Connection,
    def getConnection():Connection={
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","yzgylq")
    }

    val jdbcRdd = new JdbcRDD(sc,getConnection,"select * from iplocation where id < ? and id > ?",10,0,2,rs=>{
      val id = rs.getInt(1)
      val address = rs.getString(2)
      val accessCount = rs.getInt(3)
      (id,address,accessCount)
    })

    println(jdbcRdd.collect.toBuffer)

  }
}
