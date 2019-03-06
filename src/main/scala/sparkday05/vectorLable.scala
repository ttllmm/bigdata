package sparkday05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object vectorLable {
  def main(args: Array[String]): Unit = {
    val v1=Vectors.dense(1,2.1,3.5)
//    创建向量的标签类型 1参：向量的标签类型（double） 2参：向量
    val lb1=LabeledPoint(1,v1)
    println(lb1.features)//获取向量的标签向量
    println(lb1.label)//获取向量的标签值
//--处理labled.txt文件,最后的结果rdd[lablepoint]

    val conf=new SparkConf().setMaster("local").setAppName("vectorlable")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ml/labled.txt")

//    data.map{line=>line.split(" ").map{num=>num.toDouble}.map{arr=>LabeledPoint(arr.last,Vectors.dense(arr.take(2)))}}
  }

}
