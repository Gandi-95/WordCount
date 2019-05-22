package Test

import org.apache.spark.{SparkConf, SparkContext}

object JoinTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JoinTest").setMaster("local")
    val sc = new SparkContext(conf)

    val movie= sc.textFile("D:\\Intellij Workspace\\WordCount\\src\\main\\WordCount\\Test\\data\\movies.csv")
    val ratings = sc.textFile("D:\\Intellij Workspace\\WordCount\\src\\main\\WordCount\\Test\\data\\ratings.csv")

    val movieScores= ratings.map(line=>{
      val filds = line.split(",")
      (filds(1).toInt,filds(2).toDouble)
    }).groupByKey().map(data=>{
      val pj = data._2.sum/data._2.size
      (data._1,pj)
    }).keyBy(tup => tup._1)



    movie.map(line=>{
      val filds = line.split(",")
        (filds(0).toInt,filds(1))
    }).keyBy(tup=>tup._1).join(movieScores).filter(f => f._2._2._2 > 4.0).map(f => (f._2._1._1,f._2._1._2,f._2._2._2)).foreach(println)
  }
}
