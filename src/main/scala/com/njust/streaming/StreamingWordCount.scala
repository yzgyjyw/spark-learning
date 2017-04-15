package com.njust.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/12.
  */
object StreamingWordCount {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("streamCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc,Seconds(5))

    val stream =  ssc.socketTextStream("bigdata02",8888)

    val dstream =  stream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    dstream.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
