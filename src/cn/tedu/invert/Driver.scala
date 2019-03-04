package cn.tarena.invert

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local")
                .setAppName("invert")
    val sc=new SparkContext(conf) 
    
    //--读取指定目录下所有文件，并封装到一个RDD返回
    val data=sc.wholeTextFiles("d://data/inverted/*",2)
    
    //--提示1：需要根据路径把文档名切出来
    //--提示2：先把整个文档数据按\r\n切->line->按空格切出单词
    //--(spark,doc3)
    //--(hadoop,doc1,doc2)
    
    val parseData=data.map{case(filePath,fileText)=>
       val fileName=filePath.split("/").last.dropRight(4)
       (fileName,fileText)}
        .flatMap{case(fileName,fileText)=>
          fileText.split("\r\n")
          .flatMap { line =>line.split(" ") }
          .map{word=>(word,fileName)}
        }
        
       parseData.groupByKey
       .map { case(word,buffer) =>(word,buffer.toList.distinct.mkString(","))}
       .foreach{println}
  }
     
}