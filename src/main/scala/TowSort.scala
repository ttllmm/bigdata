//import org.apache.spark.{SparkConf, SparkContext}
//
//object TowSort {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local").setAppName("ssort1")
//    val sc = new SparkContext(conf)
//    val data = sc.textFile("e://data/ssort.txt", 3)
//    //    val ssortdata=data.map{line=>{
//    ////
//    ////      ( new Sort(line.split(" ")(0),line.split("")(1).toInt),line)
//    ////
//    ////    } }
//    ////   val result=ssortdata.sortByKey(true)
//    ////result.foreach(println(_))
//    ////  }
//    val ssortData = data.map { line => {
//      (new Sort(line.split(" ")(0), line.split(" ")(1).toInt), line)
//    }
//    }
//
//    val result = ssortData.sortByKey(true)
//
//    result.foreach(println)
//  }
//}
//class Sort(val first:String,val second:Int) extends Ordered[Sort] with Serializable {
//  override def compare(that: Sort): Int = {
//    var cp=this.first.compareTo(that.first)
//    if(cp==0){
//     that.second.compareTo(this.second)
//    }
//     else cp
//  }
//
//}
class SecondarySortKey(val first:String,val second:Int) extends Ordered[SecondarySortKey] with Serializable {

  def compare(other:SecondarySortKey):Int={
    var comp=this.first.compareTo(other.first)
    if(comp==0){
      other.second.compareTo(this.second)
    }else{
      comp
    }
  }
}



//Driver代码：
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SsortDriver {

  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local").setAppName("ssort")
    val sc=new SparkContext(conf)
    val data=sc.textFile("e://data/ssort.txt",3)

    val ssortData=data.map { line =>{
      (new SecondarySortKey(line.split(" ")(0),line.split(" ")(1).toInt),line)
    }
    }

    val result=ssortData.sortByKey(true).map(_._2)

    result.foreach(println)


  }
}