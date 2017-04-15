package com.njust.streaming.flume

import java.net.InetSocketAddress

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/13.
  */
object FlumePollWC {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("updateStateByKey").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val inetSocketAddress:Seq[InetSocketAddress] = Seq(new InetSocketAddress("bigdata02",12138))

    val dStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc,inetSocketAddress,StorageLevel.MEMORY_AND_DISK_SER_2)

    dStream.flatMap(x=>{
      new String(x.event.getBody.array()).split(" ")
    }).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
