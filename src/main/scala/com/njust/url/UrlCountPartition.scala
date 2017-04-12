package com.njust.url

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/10.
  */
object UrlCountPartition {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("urlCountPartition").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //(url,1)
    val rddLine = sc.textFile("F://weburl.log").map(line=>{
      val lineContent = line.split("\t")
      (lineContent(1),1)
    })

    //(host,(url,count))
    val rddHost = rddLine.reduceByKey(_+_).map(x=>{
      val host = new URL(x._1).getHost
      (host,(x._1,x._2))
    })

    val hosts = rddHost.map(_._1).distinct().collect

    //这个是不对的，因为已经重新分区过了，所以不能直接排序,如果是这样的话，那么最后的结果每个文件里面也是有序的，
    // 但是不同的hosts会出现在同一个文件里面
    //rddHost.partitionBy(new UrlPartition(hosts)).sortBy(_._2._2).saveAsTextFile("F://out2")
    rddHost.partitionBy(new UrlPartition(hosts)).mapPartitions(it=>{
      it.toList.sortBy(_._2._2).reverse.iterator
    }).saveAsTextFile("F://out3")

  }
}

class UrlPartition(hosts:Array[String]) extends Partitioner{

  override def numPartitions: Int = hosts.length

  override def getPartition(key: Any): Int = {
    hosts.indexOf(key)
  }
}
