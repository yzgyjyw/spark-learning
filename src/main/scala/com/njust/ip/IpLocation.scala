package com.njust.ip

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/11.
  */
object IpLocation {

  /**
    * 这样做的好处是，对于一个partition只需要一个Connection，而不是每一条数据都需要一个Connection
    * @param iterator 在调用rdd.foreachPartition的时候，其传入的函数是一个iterator
    */
  def rdd2MySql(iterator: Iterator[(String,Int)]):Unit={
    val driverClass = "com.mysql.jdbc.Driver"
    val userName = "root"
    val password = "yzgylq"
    val url = "jdbc:mysql://localhost:3306/bigdata"

    var conn:Connection = null
    var statement :PreparedStatement = null
    try{
      Class.forName(driverClass)
      conn = DriverManager.getConnection(url,userName,password)
      val sql = "insert into iplocation(address,accessCount) values(?,?)"
      iterator.foreach(it=>{
        statement = conn.prepareStatement(sql)
        statement.setString(1,it._1)
        statement.setInt(2,it._2)
        statement.executeUpdate()
      })
    }catch {
      case e:Exception=>{println("Mysql Exception")}
    }finally {
      if(statement!=null) statement.close()
      if(conn!=null) conn.close()
    }
  }



  /**
    *
    * @param lines ip校验规则
    * @param ip 待查找的ip
    * @return 待查找的ip的在校验规则中的index
    */
  def binarySearch(lines:Array[(String,String,String)],ip:Long): Int ={
    var low = 0;
    var high = lines.length-1
    while(low <= high){
      val middle = (low+high)/2
      if(lines(middle)._1.toLong <= ip && lines(middle)._2.toLong >= ip) return middle
      else if(lines(middle)._1.toLong > ip){
        high = middle - 1
      }else{
        low = middle + 1
      }
    }
    return -1
  }

  def ipToLong(ip:String): Long ={
    val ips = ip.split("\\.")
    var ipNumber = 0L
    for(i <- 0 until ips.length){
      ipNumber = ipNumber << 8 | ips(i).toLong
    }
    ipNumber
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ipLocation").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //加载ip库(start,end,address)
    val rddiptxt = sc.textFile("F://ip.txt").map(lines=>{
      val contents = lines.split("\\|")
      (contents(2),contents(3),contents(7))
    })
    //将ip库广播到所有的executors中
    val broadCastResults = sc.broadcast(rddiptxt.collect)

    //获取要查询的日志信息中的ip
    val rddSearch =  sc.textFile("F://20090121000132.394251.http.format").map(lines=>{
      val contents = lines.split("\\|")
      contents(1)
    })

    //根据ip查询地址，并按照地址统计数量
    val res = rddSearch.map(ip=>{
      val index = binarySearch(broadCastResults.value,ipToLong(ip))
      if (index != -1)  (ip,broadCastResults.value(index)._3)
      else (ip,"未知")
    }).map(x=>(x._2,1)).reduceByKey(_+_).foreachPartition(rdd2MySql(_))

    //println(res.collect().toBuffer)
    sc.stop
  }
}
