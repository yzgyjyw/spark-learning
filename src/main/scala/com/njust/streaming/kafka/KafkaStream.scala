package com.njust.streaming.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Administrator on 2017/4/13.
  */
object KafkaStream {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("kafkaStream").setMaster("local[2]")
    val sc =new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val zk = "bigdata02:2181,bigdata03:2181,bigdata04:2181"


    val topics = Map("test2"->2)

    val stream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zk,"g1",topics)

    //存储在kafka中的数据是一个K,V对，所以先取出第二个，就是value
    stream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
