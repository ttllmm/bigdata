package MLlibDay06

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.{SparkConf, SparkContext}

object LoadingDriver {
  def cosArray(a1:Array[Double],a2:Array[Double]):Unit={
    val c=a1.zip(a2)
    val  z=c.map{x=>x._1*x._2}.sum
    //    c.foreach(println(_))
    val a1mu=math.sqrt(a1.map{x=>x*x}.sum)
    val a2mu=math.sqrt(a2.map{x=>x*x}.sum)

    val cos=z/(a1mu*a2mu)
    cos

  }
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("als")
    val sc=new SparkContext(conf)
    val model=MatrixFactorizationModel.load(sc,"e://data/recomovie1/")
//    val u789=model.recommendProducts(789,10)
//    u789.foreach(println(_))
    /*
    *
    * --sparkt提供的als推荐系统，没有内置基于物品的推荐，需要自己实现
    * --我们能用的是基于用户的推荐
    * 基于物品的推荐-》通过物品来推荐物品
    * --biru：某用户看了一部电影（编号123），先在要为这个用户推荐类似10部电影
    * 处理思路：
    * 1：找出123号电影的因子值
    * 2：计算出所有电影和123号电影的相关度
    * 3：按相关度降序排序，取出前10部电影
    * */
//    --获取物品因子矩阵
    val pf=model.productFeatures
//    --获取用户因子矩阵。本例不涉及
    val uf=model.userFeatures
//    pf.foreach(println(_))
//    --以物品id为key,找出123号电影的因子值
    val movie123Factor = pf.keyBy{x=>x._1}.lookup(123).head._2
    movie123Factor.foreach{println(_)}
    val cosResult=pf.map{case(movieId,arr)=>
//    --计算出当前的电影和123号的余弦距离
      val cos=cosArray(movie123Factor,arr)
      (movieId,cos)
    }.sortBy{x=> x._2}
    val top123Movie10=cosResult.take(10)

top123Movie10.foreach(println(_))
  }


}
