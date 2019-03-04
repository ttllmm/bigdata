package cn.tedu.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Driver {
     
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("sql")
    val sc=new SparkContext(conf)
    
    //--创建SparkSql的上下文对象
    val sqc=new SQLContext(sc)
    
    val data=sc.textFile("e://data/MaxMin.txt", 2)
    
    val parseData=data.map { line => line.split(" ") }
                      .map { arr => (arr(0).toInt,arr(1),arr(2).toInt)}
    //--代码中，RDD->DataFrame需要调用sqc.createDataFrame
    val t1=sqc.createDataFrame(parseData).toDF("id","gender","high") 
    
    t1.show
    //--课后作业：用sql语句的方法，查出男性身高的最大值
    
   
    
  }
}