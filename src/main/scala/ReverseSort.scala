import org.apache.spark.{SparkConf, SparkContext}

object ReverseSort {
  def main(args: Array[String]): Unit = {
  val conf=new SparkConf().setMaster("local").setAppName("invert")
    val sc=new SparkContext(conf)
    val data=sc.wholeTextFiles("e://data/inverted/*")
    val cleaningfata=data.map{
      case (path,text)=>
        val filename=path.split("/")(4).dropRight(4)
        (filename,text)
    }
    val rd=cleaningfata.flatMap{
      case (filename,text)=>text.split("\r\n").flatMap(_.split(" ")).map(x=>(filename,x))

    }

    val result=rd.groupByKey().map{case(fn,w)=>(fn,w.toList.distinct.mkString(","))}.foreach(println(_))


}}
//object R1 {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local").setAppName("sql")
//    val sc = new SparkContext(conf)
////    --创建上下文
//    val sqc=new SQLContext(sc)
//
//    val data = sc.textFile("e://data/MaxMin.txt",2)
//    val parseData=data.map(_.split(" ")).map{arr=>(arr(0).toInt,arr(1),arr(2).toInt)}
////    --代码中，rdd-转化为datafram
//    val t1=sqc.createDataFrame(parseData).toDF("id","gender","high")
//    t1.show()
//
//
//
//
//
//  }
//}
//---课后作业：用sql查询男生身高最高