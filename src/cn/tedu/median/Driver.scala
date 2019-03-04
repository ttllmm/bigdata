package cn.tarena.median

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("median")
    
    val sc=new SparkContext(conf)
    
    val data=sc.textFile("d://data/median.txt",2)
    //--line->num(String)->num(Int)
    val parseData=data.flatMap { line=>line.split(" ") }
        .map { num => num.toInt }
    
    val medianPos=(parseData.count()+1)/2
    
    val median=parseData.top(medianPos.toInt).last
    
    println(median)
    
  }
}