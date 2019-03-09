package cn.tedu.statistic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics

object Driver {
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("statistic")
    
    val sc=new SparkContext(conf)
    
    val data=sc.textFile("e://data/ml/statistic.txt")
    
    //RDD[String]->RDD[Vectors]
    val result=data.map { x => Vectors.dense(x.toDouble)}
    //--统计一组数据的基本统计量，要求传入的类型RDD[Vectors]
    val summary=Statistics.colStats(result)
    println(summary.max)//最大值
    println(summary.min)//最小值
    println(summary.mean)//均值
    println(summary.variance)//方差
    println(summary.numNonzeros)//不是0的个数
    println(summary.normL1)//曼哈顿距离
    println(summary.normL2)//欧式距离
   
    
    
  }
}