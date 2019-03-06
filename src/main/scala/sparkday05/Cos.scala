package sparkday05

object Cos {
  def main(args: Array[String]): Unit = {
    val a1=Array(1,2,3)
    val a2=Array(4,3,6)
    val z=a1(0)*a2(0)+a1(1)*a2(1)+a1(2)*a2(2)
    val m=math.sqrt(a1(0)*a1(0)+a1(1)*a1(1)+a1(2)*a1(2))*math.sqrt(a2(0)*a2(0)+a2(1)*a2(1)+a2(2)*a2(2))
    val cos=z/m
    println(cos)

  }

}

object Cos1 {
  def main(args: Array[String]): Unit = {
    val a1=Array(1,2,3)
    val a2=Array(4,3,6)
    val c=a1.zip(a2)

    val  z=c.map{x=>x._1*x._2}.sum
    c.foreach(println(_))
    val a1mu=math.sqrt(a1.map{x=>x*x}.sum)
    val a2mu=math.sqrt(a2.map{x=>x*x}.sum)

    val cos=z/(a1mu*a2mu)
    println(cos)
//--定义一个方法cosArray（arr1，arr2），返回他们的预选距离
  }

}
object Cos3 {
  def main(args: Array[String]): Unit = {
    val a1=Array(1,2,3)
    val a2=Array(2,4,6)
    val a3=Array(3,5,7)
    val a4=Array(2,4,9)
    val a5=Array(9,2,1)
    val c=a1.zip(a2)

//    val  z=c.map{x=>x._1*x._2}.sum
//    c.foreach(println(_))
//    val a1mu=math.sqrt(a1.map{x=>x*x}.sum)
//    val a2mu=math.sqrt(a2.map{x=>x*x}.sum)
//
//    val cos=z/(a1mu*a2mu)
//    println(cos)
cosArray(a1,a2)
  }
  def cosArray(a1:Array[Int],a2:Array[Int]):Unit={
    val c=a1.zip(a2)
    val  z=c.map{x=>x._1*x._2}.sum
//    c.foreach(println(_))
    val a1mu=math.sqrt(a1.map{x=>x*x}.sum)
    val a2mu=math.sqrt(a2.map{x=>x*x}.sum)

    val cos=z/(a1mu*a2mu)
    println(cos)

  }
}





