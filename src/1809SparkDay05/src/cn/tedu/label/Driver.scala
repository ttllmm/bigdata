package cn.tedu.label

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  
  def main(args: Array[String]): Unit = {
    
    val v1=Vectors.dense(1,2.1,3.5)
    //--创建向量标签类型，①参:向量的标签值(Double)②参：向量
    val lb1=LabeledPoint(1,v1)
    
    println(lb1.features)//获取向量标签的向量
    println(lb1.label)//获取向量标签的标签值
    
    //--处理labeled.txt文件，最后的结果RDD[LabeledPoint]
    val conf=new SparkConf().setMaster("local").setAppName("label")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ml/labeled.txt")
    val result=data.map { line => line.split(" ").map { num => num.toDouble } }
                   .map { arr =>LabeledPoint(arr.last,Vectors.dense(arr.take(2))) }
    result.foreach{println}              
  }
}