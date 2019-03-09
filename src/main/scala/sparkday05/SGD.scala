package sparkday05

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

object SGD {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("SGD")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ml/testSGD.txt")
//    --接下来，为了满足需求，需要的数据类型rdd【string】-》rdd【labledpoint】
    val parseData=data.map{
      line=>
        val info=line.split(",")
        val y=info(0).toDouble//labled
        val x1=info(1).split(" ")(0).toDouble
        val x2=info(1).split(" ")(1).toDouble
        LabeledPoint(y,Vectors.dense(x1,x2))

    }

    val model=LinearRegressionWithSGD.train(parseData,20,1)
    val coef=model.weights
    val coception=model.intercept

//    val check=model.predict()
  }

}
