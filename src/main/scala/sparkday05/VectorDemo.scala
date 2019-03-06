package sparkday05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

object VectorDemo {
  def main(args: Array[String]): Unit = {
//    --创建秘籍型向量，要求类型都是double
    val v1=Vectors.dense(1.1,2.3)
    val a2=Array(1.1,2.2,3.0)
//    --通过传入array【double】创建向量
    val v2=Vectors.dense(a2)
//    --如果传入的是整型，自动转double
    val v3=Vectors.dense(1,2,3)
    val a4=Array[Double](1,2,3)
    val v4=Vectors.dense(a4)
//    --处理vector.txt。最后的形式Rdd【vector】


    val conf=new SparkConf().setMaster("local").setAppName("vector")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ml/vector.txt")
    //--rdd[string]->rdd[array[string]]->rdd[array[double]]->rdd[vector]
    val resulr = data.map{line=>line.split(" " ).map(_.toDouble)}.map{arr=>Vectors.dense(arr)}
    resulr.foreach(println(_))


//    --创建稀疏性向量.1参;向量的纬度2参：指定的向量位置下标3参：下标对应的数据
//    --稀疏向量指定了下标，开始为0
    val v5=Vectors.sparse(5,Array(0,1,3),Array(2.3,1.5,5.0))
    for (i<-0 to 4){
      println(v5(i))
    }

  }

}
