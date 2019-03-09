package MLlibDay06

import org.apache.spark.{SparkConf, SparkContext}

object FunctionSimilarity {
  def main(args: Array[String]): Unit = {

  }
  val conf=new SparkConf().setMaster("local").setAppName("fs")
  val sc=new SparkContext(conf)
//  设置用户名
  val users=sc.parallelize(Array("aaa","bbb","ccc","ddd","eee"))
//  设置电影名
  val films=sc
//def getSource():Map[String,Map[String,Int]]={
//
//
//
//
//}

}
