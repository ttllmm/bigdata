package cn.tarena.MaxMin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 处理MaxMin.txt，找到男性身高最大值对应的那一行数据
 * 比如返回：
 * 8 M 191
 */
object Driver2 {
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local")
                  .setAppName("MaxMin")
    
    val sc=new SparkContext(conf)
    val data=sc.textFile("d://data/MaxMin.txt",2)
    
    val parseData=data.map { line => line.split(" ") }
        .filter { arr => arr(1).equals("M") }
        .sortBy{arr=> -arr(2).toInt}
        .map{arr=>arr.mkString(",")}
        
    
    val result=parseData.take(3)
    result.foreach{println}
        
  }
}