package com.njust.url

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/10.
  */
object UrlCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("urlCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //(url,1)
    val rddContent = sc.textFile("F://weburl.log").map(line=>{
      val line_contents = line.split("\t")

      (line_contents(1),1)
    })

    //(host,(url,count))
    val rddCount = rddContent.reduceByKey(_+_).map(x=>{
      val host = new URL(x._1).getHost()
      (host,(x._1,x._2))
    })

    //ArrayBuffer(
    // (net.itcast.cn,CompactBuffer((net.itcast.cn,(http://net.itcast.cn/net/video.shtml,521)), (net.itcast.cn,(http://net.itcast.cn/net/course.shtml,521)), (net.itcast.cn,(http://net.itcast.cn/net/teacher.shtml,512)))),
    // (java.itcast.cn,CompactBuffer((java.itcast.cn,(http://java.itcast.cn/java/course/base.shtml,543)), (java.itcast.cn,(http://java.itcast.cn/java/video.shtml,496)), (java.itcast.cn,(http://java.itcast.cn/java/course/android.shtml,501)), (java.itcast.cn,(http://java.itcast.cn/java/course/hadoop.shtml,506)), (java.itcast.cn,(http://java.itcast.cn/java/course/cloud.shtml,1028)), (java.itcast.cn,(http://java.itcast.cn/java/teacher.shtml,482)), (java.itcast.cn,(http://java.itcast.cn/java/course/javaee.shtml,1000)), (java.itcast.cn,(http://java.itcast.cn/java/course/javaeeadvanced.shtml,477)))),
    // (php.itcast.cn,CompactBuffer((php.itcast.cn,(http://php.itcast.cn/php/course.shtml,459)), (php.itcast.cn,(http://php.itcast.cn/php/video.shtml,490)), (php.itcast.cn,(http://php.itcast.cn/php/teacher.shtml,464)))))
    //也就是(host,CompactBuffer(原数据，原数据，原数据，...)
    println(rddCount.groupBy(_._1).collect.toBuffer)

    //有一个缺点，就是当数据很大的时候，这里采用的排序方式是java的，就是将数据全部读取到内存中，然后再进行排序
    val res = rddCount.groupBy(_._1).mapValues(iter=>{
      //groupBy之后的数据格式为
      iter.toList.sortBy(_._2._2).reverse.take(2)
    })

    //思考：如果直接向下面这样写对不对,这样排序的是按照host排序，而不是按照每个host的url的访问次数
    //val res = rddCount.groupBy(_._1).sortBy()

    println(res.collect.toBuffer)
    sc.stop()
  }
}
