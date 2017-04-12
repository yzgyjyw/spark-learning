package com.njust.ip

import java.io._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/4/11.
  */
object IpLocationDemo {

  //1.0.1.0
  def ipToLong(ip:String):Long ={
    val ips = ip.split("\\.")
    var ipNumber = 0L
    for(i <- 0 until ips.length){
      ipNumber = ipNumber << 8 | ips(i).toLong
    }
    ipNumber
  }

  def readData():ArrayBuffer[String]={
//    val inStream = new FileInputStream("F://ip.txt")
//    val br = new BufferedReader(new InputStreamReader(inStream))
//    val lines = new util.ArrayList[String]()
//    var line:String = null
//    while ((line = br.readLine())!=null){
//      lines.add(line)
//    }
//    br.close()
//    inStream.close()
//    lines
val br = new BufferedReader(new InputStreamReader(new FileInputStream("F://ip.txt")))
    var s: String = null
    var flag = true
    val lines = new ArrayBuffer[String]()
    while (flag)
    {
      s = br.readLine()
      if (s != null)
        lines += s
      else
        flag = false
    }
    lines
  }

  def binarySearch(lines:ArrayBuffer[String],ip:Long):Int={
    var index = -1
    var low = 0
    var high = lines.size
    while(low<=high){
      val middle = (high+low) / 2
      val currentLines =  lines(middle).split("\\|")
      if(currentLines(2).toLong <= ip && currentLines(3).toLong >= ip) return middle
      else if(currentLines(2).toLong > ip){
        high = middle-1
      }else{
        low = middle + 1
      }
    }
    index
  }


  def main(args: Array[String]) {
    val ip = "1.10.0.0"
    val lines = readData()
    val index = binarySearch(lines,ipToLong(ip))
    println(lines(index))
  }
}
