package cn.tedu.vector

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  
  def main(args: Array[String]): Unit = {
    
    //--创建密集型向量,要求数据类型是Double
    val v1=Vectors.dense(1.1,2.3,3.5)
    
    val a2=Array(1.1,2.2,3.0)
    //--通过传入Array[Double] 创建向量
    val v2=Vectors.dense(a2)
    //--如果传入的是整型,自动转Double
    val v3=Vectors.dense(1,2,3)
    
    val a4=Array[Double](1,2,3)
    val v4=Vectors.dense(a4)
    //--处理vector.txt。最后的形式RDD[Vectors]
    
    val conf=new SparkConf().setMaster("local").setAppName("vector")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ml/vector.txt")
    //--RDD[String]->RDD[Array[String]]->RDD[Array[Double]]->RDD[Vectors]
    val result=data.map { line => line.split(" ")
                   .map { num =>num.toDouble } }
                   .map { arr => Vectors.dense(arr) }
    
    //--创建稀疏向量,①参:向量的维度 ②参:指定的向量位置下标  ③参:下标对应的数据
    //--稀疏向量，除了指定下标有数据，其他都是0                           
    val v5=Vectors.sparse(5,Array(0,1,3),Array(2.1,1.5,5.0)) 
    for(i<-0 to 4){
      println(v5(i))
    }
  }
}