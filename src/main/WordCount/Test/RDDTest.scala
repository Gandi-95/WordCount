package Test

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object RDDTest {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("PartitionerTest").setMaster("local")
      val sc = new SparkContext(conf)

//      TuShu(conf,sc)
      readjson(sc)


  }

  def TuShu(conf:SparkConf,sc:SparkContext):Unit={
    val rdd = sc.parallelize(Array(("spark",2),("hadoop",6),
      ("hadoop",4),("spark",6)))
    rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>(x._1/x._2))
      .collect().foreach(println)
  }

  def readjson(sc:SparkContext): Unit ={
    val jsonStr = sc.textFile("D:\\Intellij Workspace\\WordCount\\src\\main\\WordCount\\data\\people")
    val resut = jsonStr.map(s=>JSON.parseFull(s))
    resut.foreach(r => r match {
      case Some(map: Map[String, Any]) => println(map)
      case None => println("None")
      }
    )

  }
}
