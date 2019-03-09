package cn.tedu.cos

object Driver {
  
  def main(args: Array[String]): Unit = {
    
    val a1=Array(1,2,3)
    val a2=Array(2,4,6)
    val a3=Array(3,5,7)
    val a4=Array(1,4,9)
    val a5=Array(9,2,1)
    
    //--计算a1和a2向量的余弦距离
    //--提示:zip(拉链方法)
    //--提示:开方:Math.sqrt()
    println(cosArray(a1, a2))
    println(cosArray(a1, a3))
    println(cosArray(a1, a4))
    println(cosArray(a1, a5))
    
  }
  
  def cosArray(a1:Array[Int],a2:Array[Int])={
     val a1a2=a1 zip a2
     val a1a2Fenzi=a1a2.map{x=>x._1*x._2}.sum
     val a1Fenmu=Math.sqrt(a1.map { x => x*x }.sum) 
     val a2Fenmu=Math.sqrt(a2.map { x => x*x }.sum) 
     val a1a2Cos=a1a2Fenzi/(a1Fenmu*a2Fenmu)
     a1a2Cos 
  }
}