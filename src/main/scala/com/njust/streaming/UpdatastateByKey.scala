package com.njust.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Created by Administrator on 2017/4/13.
  */
object UpdatastateByKey {

  /*def updateStateByKey[S: ClassTag](
  updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
  partitioner: Partitioner,
  rememberPartitioner: Boolean): DStream[(K, S)]*/

  //(hello,(1,1,1)) (tom,(1,1,1,1))
  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])]) => {
    iter.map(it=>{
      (it._1,it._2.sum+it._3.getOrElse(0))
    })
  }


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("updateStateByKey").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata02",8888)

    sc.setCheckpointDir("F:/out00")

    //使用upcateStateByKey的时候，其内部会进行checkpoint，因为它是一个累加history计算的过程，会将每次计算的结果进行checkpoint
    val dStream1: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),true)


    dStream1.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
