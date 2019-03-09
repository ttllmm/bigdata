package MLlibDay06


import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
/*
*
* 逻辑回归模型解决的不是预测问题，而是二分类问题
* 要明确适用的场景是数据集中因变量Y离散的（取值个数是有限的）
*
*
* */
object Accident {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("accident")
    val sc=new SparkContext(conf)
//    val sqc=new SQLContext(sc)
    val data=sc.textFile("e://data/ml/logistic.txt")
//    为了满足建模需求，rdd【string】-》rdd【lablepoint】
    val parseData=data.map{

      line=>line.split("\t").map{num=>num.toDouble}}
          .map{arr=>LabeledPoint(arr.last,Vectors.dense(arr.take(3)))}
//    建立模型

//    用拟牛顿发来解系数，此算法也是通过
    val model=new LogisticRegressionWithLBFGS().setNumClasses(2).run(parseData)
//    val model=LogisticRegressionWithSGD.train(parseData,20,0.05)
    println(model.weights)
    println(model.intercept)
    val predict=model.predict(parseData.map({_.features}))
    predict.foreach(println(_))






  }

}
