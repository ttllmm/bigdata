package MLlibDay06

import org.apache.spark.{SparkConf, SparkContext}

object Similar {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("similar")
    val sc = new SparkContext(conf)
    val user1FilmSource = Map("m1" -> 2, "m2" -> 3, "m3" -> 1, "m4" -> 0, "m5" -> 1)

    val user2FilmSource = Map("m1" -> 1, "m2" -> 2, "m3" -> 2, "m4" -> 1, "m5" -> 4)

    val user3FilmSource = Map("m1" -> 2, "m2" -> 1, "m3" -> 0, "m4" -> 1, "m5" -> 4)

    val user4FilmSource = Map("m1" -> 3, "m2" -> 2, "m3" -> 0, "m4" -> 5, "m5" -> 3)

    val user5FilmSource = Map("m1" -> 5, "m2" -> 3, "m3" -> 1, "m4" -> 1, "m5" -> 2)
    val fenzi12 = user1FilmSource.toVector.zip(user2FilmSource).map(d => d._1._2 * d._2._2).reduce(_ + _).toDouble
    val fenzi13 = user1FilmSource.toVector.zip(user3FilmSource).map(d => d._1._2 * d._2._2).reduce(_ + _).toDouble
    val fenzi14 = user1FilmSource.toVector.zip(user4FilmSource).map(d => d._1._2 * d._2._2).reduce(_ + _).toDouble
    val fenzi15 = user1FilmSource.toVector.zip(user5FilmSource).map(d => d._1._2 * d._2._2).reduce(_ + _).toDouble

    val user1_fenmu = math.sqrt(user1FilmSource.map { case (k, v) => math.pow(v, 2) }.reduce(_ + _))
    val user2_fenmu = math.sqrt(user2FilmSource.map { case (k, v) => math.pow(v, 2) }.reduce(_ + _))
    val user3_fenmu = math.sqrt(user3FilmSource.map { case (k, v) => math.pow(v, 2) }.reduce(_ + _))
    val user4_fenmu = math.sqrt(user4FilmSource.map { case (k, v) => math.pow(v, 2) }.reduce(_ + _))
    val user5_fenmu = math.sqrt(user5FilmSource.map { case (k, v) => math.pow(v, 2) }.reduce(_ + _))

    val cos12 = fenzi12 / (user1_fenmu * user2_fenmu)
    val cos13 = fenzi12 / (user1_fenmu * user2_fenmu)
    val cos14 = fenzi12 / (user1_fenmu * user2_fenmu)
    val cos15 = fenzi12 / (user1_fenmu * user2_fenmu)

    println(s"1和2的相似度为：$cos12")
    println(s"1和3的相似度为：$cos13")
    println(s"1和4的相似度为：$cos14")
    println(s"1和5的相似度为：$cos15")


  }
}
