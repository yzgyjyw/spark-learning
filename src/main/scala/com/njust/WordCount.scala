package com.njust

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/7.
  */
object WordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(sparkConf)
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_,1).sortBy(_._2,false).saveAsTextFile(args(1))
    sc.stop()
  }
}
