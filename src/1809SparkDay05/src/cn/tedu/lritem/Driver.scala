package cn.tedu.lritem

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

/**
 * 基本概念：
 * 1.线性回归模型 
 * 线性指的是:函数方程是线性方程，比如直线方程,平面方程
 * Y=β1X1+β0 直线方程
 * Y=β1X1+β2X2+β0 平面方程
 * Y=β1X1+β2X2+……βnXn+β0 超平面方程
 * 
 * 2.回归模型,用于预测。本例中用的是最小二乘回归
 * 即用的是最小二乘法来解的方程
 * 
 * 3.一元线性回归
 * Y=β1X1+β0 一元
 * 
 * 4.多元线性回归
 * Y=β1X1+β2X2+……βnXn+β0 自变量有多个
 * 
 * 5.因为非线性方程的形式不确定，很难求解。所以一般使用线性方程来解决
 */
object Driver {
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("lritem")
    val sc=new SparkContext(conf)
    val sqc=new SQLContext(sc)
    val data=sc.textFile("e://data/ml/lritem.txt")
    
    //--RDD[String]->RDD[(X1,X2,Y)]->DataFrame(X1,X2,Y)
    val parseData=data.map { line =>
      val info=line.split("\\|")
      val Y=info(0).toDouble
      val X1=info(1).split(" ")(0).toDouble
      val X2=info(1).split(" ")(1).toDouble
      (X1,X2,Y)
    }
    //--RDD->DataFrame
    val df=sqc.createDataFrame(parseData).toDF("X1","X2","Y")
    //--为了满足后续的建模需求，需要把DF转成指定的向量形式
    val ass=new VectorAssembler()
                //--指定哪些列是自变量
                .setInputCols(Array("X1","X2"))
                //--为自变量起一个别名
                .setOutputCol("features")
    //--转成模型要求的向量类型            
    val dfVector=ass.transform(df) 
    
    //--建立线性回归方程，用于预测
    val model=new LinearRegression()
                  //--设定自变量列
                  .setFeaturesCol("features")
                  //--设定因变量列
                  .setLabelCol("Y")
                  //--true表示计算方程截距项系数
                  .setFitIntercept(true)
                  //--代入数据,计算出模型
                  .fit(dfVector)
     //--获取模型的系数             
     val coef=model.coefficients  
     //--获取截距项系数
     val intercept=model.intercept
     //--Y=-6.497X1+0.016X2+106.36
     
     //--获取模型的多元R方值,此值最大为1,越大说明模型对于数据的拟合越好
     //--生产环境下，一般0.6~0.7算比较高的
     val r=model.summary.r2
     
     //--接下来做模型的预测,要求数据类型：DataFrame[vector]
     val predictResult=model.transform(dfVector)
     
     val testData=sc.textFile("e://data/ml/predictitem.txt")
     val testRDD=testData.map { line =>
       val X1=line.split(" ")(0).toDouble
       val X2=line.split(" ")(1).toDouble
       (X1,X2,0)
     }
    val testDF=sqc.createDataFrame(testRDD).toDF("X1","X2","Y")
    val testVector=ass.transform(testDF)
    
    val testPredict=model.transform(testVector)
    val testResult=testPredict.selectExpr("X1","X2","round(prediction,3)")
    val resulRDD=testResult.toJavaRDD
    resulRDD.saveAsTextFile("e://data/ml/lrresult")
     
  }
}