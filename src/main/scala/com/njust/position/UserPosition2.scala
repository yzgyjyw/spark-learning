package com.njust.position

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/8.
  */
object UserPosition2 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("UserPosition").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //（(number,base_location),time）
    val pbt = sc.textFile("F:\\user_position.txt").map(line=>{
      val lines = line.split(",")
      //1：接入  0：离开
      val time = if (lines(3)=="1") -lines(1).toLong else lines(1).toLong
      ((lines(0),lines(2)),time)
    })

    val res = pbt.reduceByKey(_+_).groupBy(_._1._1).mapValues(_.toList.sortBy(_._2).reverse)

    println(res.collect.toBuffer)
  }
}
