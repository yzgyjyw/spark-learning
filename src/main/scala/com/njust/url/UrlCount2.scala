package com.njust.url

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/10.
  */
object UrlCount2 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("urlCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //(url,1)
    val rddContent = sc.textFile("F://weburl.log").map(line=>{
      val line_contents = line.split("\t")
      (line_contents(1),1)
    })

    //(host,url,count)
    val rddCount = rddContent.reduceByKey(_+_).map(x=>{
      val host = new URL(x._1).getHost()
      (host,x._1,x._2)
    })

    //比之前的一种方法的好处是，使用rdd的sortBy而不是java的集合的，在spark中要是内存放不下的话，会先将数据存放在磁盘里面
    val rddJava = rddCount.filter(_._1.equals("java.itcast.cn"))

    val res = rddJava.sortBy(_._3,false)

    println(res.collect.toBuffer)

  }
}
