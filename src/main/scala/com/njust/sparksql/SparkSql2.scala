package com.njust.sparksql

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/12.
  * 使用指定的Schema的方式操作
  * 1.  指定Schema StructType(Array[StructField])
  * 2.  创建rowRdd Row(,,...)
  * 3.  将指定的schema映射到rowRdd上 sqlContext.createDataFrame(rowRdd,schema)
  */
object SparkSql2 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("urlCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val schema = StructType(Array[StructField](
      StructField("id",DataTypes.IntegerType,false),
      StructField("name",DataTypes.StringType,true),
      StructField("age",DataTypes.IntegerType,true)
    ))

    val rowRdd = sc.textFile("hdfs://bigdata02:9000/person.txt").map(x=>{
      val xs = x.split(",")
      Row(xs(0).toInt,xs(1),xs(2).toInt)
    })

    //将schema信息映射到rowRdd上
    val personDataFrame =  sqlContext.createDataFrame(rowRdd,schema)
    personDataFrame.registerTempTable("t_person")
    sqlContext.sql("select * from t_person").write.json("F://outPerson")

    sc.stop()
  }
}
