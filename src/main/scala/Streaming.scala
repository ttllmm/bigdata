import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.kafka.KafkaUtils

object Streaming {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("streaming")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))
//    设置检查点目录
    ssc.checkpoint("e;//streamcheck")
//    val data=ssc.textFileStream("hdfs://hadoop03/stream")
//    --监听socket数据源
    val data=ssc.socketTextStream("hadoop03",8888)
//    val result=data.flatMap(_.split(" ")).map{(_,1)}.reduceByKey(_+_)
//    历史数据叠加
    val result=data.flatMap(_.split(" ")).map{(_,1)}.updateStateByKey{(seq,op:Option[Int])=>Some(seq.sum+op.getOrElse(0))}
        result.print()
    ssc.start()
//    --保持
    ssc.awaitTermination()
//    val l=KafcaUtils.
  }

}
