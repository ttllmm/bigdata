import org.apache.spark._
object midValueDriver{
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("searchMid")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/median.txt")
    val cm=(data.flatMap(_.split(" ")).count()+1)/2
//    val mv=data.flatMap(_.split(" ")).map(_.toInt).takeOrdered(cm.toInt).last
    val mv=data.flatMap(_.split(" ")).map(_.toInt).sortBy(x=>x)
    val m= mv.foreach(print(_)+"\t")
    val mmv=mv.take(cm.toInt).last

   print(s"个数 $cm 排好的是$m")
  println(s"中位数是 $mmv")




  }

}
