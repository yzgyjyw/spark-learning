package com.njust.position

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/8.
  */
object UserPosition {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("UserPosition").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //(18611132889_9F36407EAD0629FC166F14DDE7970F68,-20160327075000)...
    val pbt = sc.textFile("F:\\user_position.txt").map(line=>{
      val lines = line.split(",")
      //1：接入  0：离开
      val time = if (lines(3)=="1") -lines(1).toLong else lines(1).toLong
      (lines(0)+"_"+lines(2),time)
    })

    val pbd = pbt.groupBy(_._1).mapValues(_.foldLeft(0L)(_+_._2))

    val res = pbd.map(x=>{
      val mobile = x._1.split("_")(0)
      val base_station = x._1.split("_")(1)
      (mobile,base_station,x._2)
    }).groupBy(_._1).mapValues(it=>{
      it.toList.sortBy(_._3).reverse.take(2)
    })

    println(res.collect.toBuffer)

    sc.stop()
  }
}
