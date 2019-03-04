package cn.tarena.average

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local")
                .setAppName("average")
    val sc=new SparkContext(conf)
    
    val data=sc.textFile("d://data/average.txt",2)
    
    //--line->Array[String]->String(num).toInt
    val parseData=data.map { line =>line.split(" ") }
        .map { arr => arr(1).toInt }
    
    val sum=parseData.sum
    val count=parseData.count
    val average=sum/count
    print(average)
  }
}