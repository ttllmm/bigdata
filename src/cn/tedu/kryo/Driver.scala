package cn.tedu.kryo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object Driver {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local")
      .setAppName("kryoTest")
      .set("spark.serializer", 
          "org.apache.spark.serializer.KryoSerializer")
      //--设置自定义的kryo注册器
      .set("spark.kryo.registrator",
          "cn.tedu.MyKryoRegister")
    val sc = new SparkContext(conf)
    
    
    val p1=new Person("tom",23)
    val p2=new Person("rose",25)
    val r1=sc.makeRDD(List(p1,p2))
    r1.persist(StorageLevel.MEMORY_ONLY_SER)
    
    r1.unpersist()

  }
}