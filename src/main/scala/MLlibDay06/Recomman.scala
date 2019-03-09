package MLlibDay06

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object Recomman {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("als")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ml/als.txt")
//    --为了满足建模需求。需要数据类型：rdd【rating（userid（int），itemid（int），score（double））】
    val parseData=data.map{

      line=>
        val info=line.split(" ")
       val userId=info(0).toInt
       val itemId=info(1).toInt
       val score=info(2).toDouble
        Rating(userId,itemId,score)

    }
    parseData.foreach(println(_))

   /*
   *
   * 1参：数据
   * 2参：隐藏因子k，需要注意：k是地接的，一般取值范围在10~50之间
   * k取值越大，意味着计算代价越大。
   * 3参：最大迭代次数
   * 4参：lamda 防止模型的过拟合，一般取很小的一个值，太大会增大误差
   * 下面的模型是通过ALS（交替最小二乘法）而建立的推荐系统模型
   * */
    val model=ALS.train(parseData,3,10,0.01)
//    --下面表示为标号为1的用户推荐3个商品
    val u1result = model.recommendProducts(1,3)
    u1result.foreach(println(_))
//    --下面表示为11号商品推荐两个用户
val itemresult = model.recommendUsers(11,2)
  println(Math.abs("console-consumer-2857".hashCode()) % 50)
  }

}
