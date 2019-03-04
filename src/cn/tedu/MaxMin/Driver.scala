package cn.tarena.MaxMin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local")
                  .setAppName("MaxMin")
    
    val sc=new SparkContext(conf)
    val data=sc.textFile("d://data/MaxMin.txt",2)
    
    //--line->Array[String]->Filter过滤出男性数据->身高数据
    val parseData=data.map { line => line.split(" ") }
        .filter { arr => arr(1).equals("M") }
        .map { arr => arr(2).toInt }
    
    val max=parseData.max
    println(max)
        
                  
                  
  }
}