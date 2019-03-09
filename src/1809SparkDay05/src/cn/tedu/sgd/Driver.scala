package cn.tedu.sgd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object Driver {
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("sgd")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ml/testSGD.txt")
    
    //--接下来，为了满足建模需要，需要的数据类型RDD[String]->RDD[LabeledPoint]
    val parseData=data.map { line =>
      val info=line.split(",")
      val Y=info(0).toDouble
      val X1=info(1).split(" ")(0).toDouble
      val X2=info(1).split(" ")(1).toDouble
      LabeledPoint(Y,Vectors.dense(X1, X2)) 
    }
    //--建模
    //--步长的选取很重要，一般在0.01~0.5
    //--下面的模型表示的是用随机梯度下降法(SGD)来解线性回归方程
    val model=LinearRegressionWithSGD.train(parseData,20,1)
    //--获取模型系数
    val coef=model.weights
    //--获取截距项系数
    val intercept=model.intercept
    //--Y=0.99X1+1.002X2
    
    //--预测，模型要求传入的数据结果RDD[Vector(自变量)]
    val predictResult=model.predict(parseData.map{x=>x.features })
    predictResult.foreach{println}
  }
}