package cn.tedu.check

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("check")
    
    val sc=new SparkContext(conf)
    //--设置检查点目录,当RDD缓存失效时，可以从检查点目录恢复
    sc.setCheckpointDir("e://check")
    
    val data=sc.textFile("e://data/word.txt",2)
    data.cache()
    
    val r1=data.flatMap { _.split(" ") }
    val r2=r1.map {(_,1) }
    r2.cache()
    r2.checkpoint()
    val r3=r2.reduceByKey(_+_)
    
    r3.foreach{println}
    
    data.unpersist()
    r2.unpersist()
    
  }
}