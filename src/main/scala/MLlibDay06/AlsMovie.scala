package MLlibDay06

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object AlsMovie {
  def main(args: Array[String]): Unit = {
//    --处理u.data文件，建立推荐系统，为编号为789号用户推荐10部电影
val conf=new SparkConf().setMaster("local").setAppName("als")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ml/u.data")
//    读取电影信息
    val movieData =sc.textFile("e://data/ml/u.item")
//    --处理moviedata rdd【string】=》rdd【（movieid，moviename）】-》map（key，vale）
    val movieMap=movieData.map{
      line=>
        val info = line.split("\\|")
        val movieId=info(0).toInt
        val movieName=info(1)
        (movieId,movieName)

    }.collectAsMap()


    //    --为了满足建模需求。需要数据类型：rdd【rating（userid（int），itemid（int），score（double））】
    val parseData=data.map{

      line=>
        val info=line.split("\t")
        val userId=info(0).toInt
        val itemId=info(1).toInt
        val score=info(2).toDouble
        Rating(userId,itemId,score)

    }

    /*
    *
    * 1参：数据
    * 2参：隐藏因子k，需要注意：k是递减的，一般取值范围在10~50之间
    * k取值越大，意味着计算代价越大。
    * 3参：最大迭代次数
    * 4参：lamda λ大会增大误差
    * 下面的模型是通过ALS（交替最小二乘法）而建立的推荐系统模型
    * */
    val model=ALS.train(parseData,50,10,0.01)
    //    --下面表示为标号为789喜欢的的用户推荐10个商品
    val u789 = model.recommendProducts(789,10).map{
      r=>
        val userId=r.user
//        --获取物品Id
        val movieId=r.product
        val movieName=movieMap(movieId)
        val score=r.rating
        (userId,movieName,score)


    }
//    val pre=model.predict(789,10)

//    println(pre)

//    u1result.foreach(println(_))
//    movieMap.foreach(println(_))
/*
*  接下来做模型验证
*  处理思路：1.先找出789号用户看过的所有电影
* 2.通过打分，找出
* */
//    --以用户id为key，具体找出789所有数据
val u789Movies=parseData.keyBy{ x=>x.user}.lookup(789)
//    u789Movies.foreach(println(_))
    val u789Top10=u789Movies.sortBy{x=> -x.rating}.take(10).map{x=>
      val userId=x.user
      val movieId=x.product
      val movieName=movieMap(movieId)
      val score=x.rating
      (userId,movieId,score)
    }
//    u789Top10.foreach(println(_))
//
model.save(sc,"e://data/recomovie1/")
  }


}
