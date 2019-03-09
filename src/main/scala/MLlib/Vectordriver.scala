package MLlib

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors

object Vectordriver {
  def main(args: Array[String]): Unit = {
    val vd=Vectors.dense(1,2,4,5)
//    println(vd)
    //①参:size。spare方法是将给定的数据Array数据(9,5,2,7)分解成指定的size个部分进行处理，本例中是7个
    //③参:输入数据。本例中是Array（9,5,2,7)
    //②参:输入数据对应的下标，要求递增，并且最大值要小于等于size
    val vs=Vectors.sparse(7,Array(1,3,4,5,6),Array(1.0,3.0,5,6,7))
//    println(vs(7))

    LabeledPoint(vd(0).toDouble,Vectors.dense(vd(1).toDouble,vd(2).toDouble))



//val conf=new SparkConf().setMaster("local").setAppName("vd")
//    val sc=new SparkContext(conf)
//
//    val data=sc.textFile("d://ml/labeled.txt")
//
//    val parseData=data.map { x =>
//      val parts=x.split(" ")
//
//      LabeledPoint(parts(2).toDouble,Vectors.dense(parts(0).toDouble,parts(1).toDouble))
//    }

  }

}
