package com.njust.streaming.flume


import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

/**
  * Created by Administrator on 2017/4/13.
  */
object FlumePushWC {

  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])])=> {
    iter.map(it=>{
      (it._1,it._2.sum+it._3.getOrElse(0))
    })
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("updateStateByKey").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(5))

    sc.setCheckpointDir("F://out00")

    val stream = FlumeUtils.createStream(ssc,"192.168.1.100",12138)

    val stream1: DStream[(String, Int)] = stream.flatMap(x => new String(x.event.getBody.array()).split(" ")).map((_, 1))
      .updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)

    stream1.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
