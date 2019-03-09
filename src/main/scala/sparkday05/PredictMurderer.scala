package sparkday05

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object PredictMurderer {


  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("predictMurderer")
    val sc=new SparkContext(conf)
    val sqc=new SQLContext(sc)
    val data1=sc.textFile("e://data/ml/lrmurder-sample.txt")
    val map_data=data1.map{
      line=>
        val data_list = line.split(",")
        (data_list(0).toDouble,
          data_list(1).toDouble,
          data_list(2).toDouble,
          data_list(3).toDouble,
          data_list(4).toDouble,
          data_list(5).toDouble,
          data_list(6).toDouble,
          data_list(7).toDouble)
    }
    //--rdd转化成df形式
    val df=sqc.createDataFrame(map_data)
    val data= df.toDF("Population", "Income", "Illiteracy", "LifeExp",
      "Murder", "HSGrad", "Frost", "Area")
    val colArray = Array("Population", "Income", "Illiteracy","LifeExp",
      "HSGrad", "Frost", "Area")
    //--为满足后续需求，需建立向量并转化成需要的类型
    val assemble=new VectorAssembler().
      setInputCols(colArray).//设置自变量
      setOutputCol("features")//给输出结果起个别名

    //  转化成模型需要的类型
    val vectorDF=assemble.transform(data)

    //--建立线性回归方程，并设置参数
    val model=new LinearRegression()
      //    设置自变量列
      .setFeaturesCol("features")
      //    设置因变量列
      .setLabelCol("Murder")
      //  设置截距项系数
      .setFitIntercept(true)

//      --正则化
      //      --设置最大迭代次数
      .setMaxIter(10)

//      设置正则化参数
      .setRegParam(0.3)
//    含义：弹性网络混合参数，范围[0,1]
      .setElasticNetParam(0.8)
      //  带入数据，计算出模型
      .fit(vectorDF)


    model.extractParamMap()//获取模型数据
//    coefficients为未知数系数
//    intercept为截距即常量值
//   结果 Coefficients: [1.3666219977814578E-4,0.0,1.1834384307110986,-1.4580829641781168,0.0,-0.01068643427005255,4.05135505051055E-6] Intercept: 109.58965988164056
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")



//    模型的评估
//    获取模型的r方值

//    val r=model.summary.r2//结果为0.7920794990832932
    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")


//    预测结果
val testData=sc.textFile("e://data/ml/lrmurder-test.txt")
    val testRdd=testData.map{
      line=>
        val data_list = line.split(",")
        (data_list(0).toDouble,
          data_list(1).toDouble,
          data_list(2).toDouble,
          data_list(3).toDouble,
          data_list(4).toDouble,
          data_list(5).toDouble,
          data_list(6).toDouble,
          data_list(7).toDouble)
    }

    val testDF=sqc.createDataFrame(testRdd).toDF("Population", "Income", "Illiteracy", "LifeExp",
      "Murder", "HSGrad", "Frost", "Area")
    val testVector=assemble.transform(testDF)//转化成需要的格式
//    利用模型进行预测
val predictions = model.transform(testVector)
    println("输出预测结果")
    val predict_result =predictions.selectExpr("Murder", "round(prediction,1) as prediction")
    predict_result.foreach(println(_))

  }

















}
