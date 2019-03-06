package Hbasedemo
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.RandomRowFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.spark.{SparkConf, SparkContext}
object SPARKHBASE {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local")
      .setAppName("park-hbase")

    val sc=new SparkContext(conf)
    val hbaseConf=HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
//    --指定输出hbase的表名
      hbaseConf.set(TableInputFormat.INPUT_TABLE,"tabx")
    val scan=new Scan()
    scan.setFilter(new RandomRowFilter(0.5f))
    //--设置scan对象，让filter生效
    hbaseConf.set(TableInputFormat.SCAN,
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    val resultRDD=sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],classOf[Result])

    resultRDD.foreach{x=>{
      //--查询出来的结果集存在 (ImmutableBytesWritable, Result)第二个元素
      val result=x._2
      //--获取行键
      val rowKey=Bytes.toString(result.getRow)

      val name=Bytes.toString(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("name")))
      val age=Bytes.toString(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("age")))

      println(rowKey+":"+name+":"+age)

    }}
  }

}
