package cn.tarena.ssort

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("ssort")
    val sc=new SparkContext(conf)
    
    val data=sc.textFile("d://data/ssort.txt", 2)
    
    //--1.line->(ssort,line)
    //--2.sortByKey-按ssort对象的compare实现二次排序
    
    val parseData=data.map { line =>
      val infos=line.split(" ")
      val col1=infos(0)
      val col2=infos(1).toInt
      (new Ssort(col1,col2),line)
    }
    
    parseData.sortByKey(true)
             .map{x=>x._2}foreach{println}
  }
}