package sparkday05

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object PredicateDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("predicate")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ml/lritem.txt")
//    --Rdd[string]->rdd[(x1,x2,y)]->dataframe(x1,x2,y)
   val  sqc=new SQLContext(sc)
    val parseData=data.map{line=>
      val info=line.split("\\|")
      val y=info(0).toDouble
      val x1=info(1).split(" ")(0).toDouble
      val x2=info(1).split(" ")(1).toDouble
      (x1,x2,y)

    }
val df=sqc.createDataFrame(parseData).toDF("X1","X2","Y")
    df.show()

//    -- 为了满足后续建模需要，需要把df转化成指定的向量形式
    val  ass=new VectorAssembler()
//    指定哪些列是自变量
      .setInputCols(Array("X1","X2"))
//    --为自变量起一个名字
      .setOutputCol("features")
//    --转成模型要求的向量类型
    val  dfVector=ass.transform(df)
    val model=new LinearRegression()
      //    --设置自变量
      .setFeaturesCol("features")
      //    --设置因变量

      .setLabelCol("Y")
//      --true表示计算方程截距项系数
      .setFitIntercept(true)
//      --带入数据，计算出模型
      .fit(dfVector)
//    --获取模型的系数
    val coef=model.coefficients
//    --获取截距项系数
    val intercept=model.intercept
//    --Y=-6.497X1+0..16+106.368
//    println(coef)
//    println(intercept)
//    -- 获取模型的的多元r方值，越大说明模型对于数据的拟合越好
//    --生产环境下，一般0.6~0.7比较好
    val r2=model.summary.r2
//--接下来做模型的预测,将原数据带入公式中，要求数据类型datafram【vector】
   val predictResult= model.transform(dfVector)
//    predictResult.show()


val testdata=sc.textFile("e://data/ml/predictitem.txt")
val testRDD=testdata.map{

  line=>
    val x1=line.split(" ")(0).toDouble
    val x2=line.split(" ")(1).toDouble
    (x1,x2,0)

}

    val testDF=sqc.createDataFrame(testRDD).toDF("X1","X2","Y")
    val testVector=ass.transform(testDF)
    val testPredict=model.transform(testVector)
    val testResult=testPredict.selectExpr("X1","X2","round(prediction,3)")
    val resultRdd=testResult.toJavaRDD
    resultRdd.saveAsTextFile("e://data/ml/i90.txt")



  }
}
