package com.njust.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/12.
  * 使用的是通过反射推断Schema的方法来完成SparkSql
  * 1.  DSL风格
  * 2.  Sql风格
  */
object SparkSql {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("urlCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext =  new SQLContext(sc)

    val personRdd = sc.textFile("hdfs://bigdata02:9000/person.txt").map(x=>{
      val xs = x.split(",")
      Person(xs(0).toInt,xs(1),xs(2).toInt)
    })

    import sqlContext.implicits._
    //使用DSL风格的语法
    val personDF = personRdd.toDF
    personDF.show()

    //使用Sql风格的语法
    personDF.registerTempTable("t_person")
    sqlContext.sql("select * from t_person order by age limit 2").write.json("F://person.txt")
    sc.stop
  }
}

case class Person(id:Int,name:String,age:Int)
