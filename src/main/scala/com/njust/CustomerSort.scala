package com.njust

import org.apache.spark.{SparkConf, SparkContext}


//隐式转换，将Girl类型转换为Ordering类型
object OrderContext{
  implicit val girlOrdering = new Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue!=y.faceValue) x.faceValue - y.faceValue
      else y.age - x.age
    }
  }
}


/**
  * Created by Administrator on 2017/4/10.
  */
object CustomerSort {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("CustomerSort").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val girls = sc.parallelize(List(("jyw",90,23),("lq",99,24),("xzz",99,25)))
    import OrderContext._
    val sorted = girls.sortBy(x=>Girl(x._2,x._3),false)
    println(sorted.collect.toBuffer)
    sc.stop()
  }
}

/**
  * 第一种自定义排序的方法
  * 定义一个类，继承Ordered，然后实现Serializable
  * @param faceValue
  * @param age
  */
/*case class Girl(faceValue:Int,age:Int) extends Ordered[Girl] with Serializable{
  override def compare(that: Girl): Int = {
    if(this.faceValue!=that.faceValue) this.faceValue - that.faceValue
    else that.age - this.age
  }
}*/

case class Girl(faceValue:Int,age:Int) extends Serializable