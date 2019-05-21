package Test

import org.apache.spark.{SparkConf, SparkContext}

object JoinTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JoinTest").setMaster("local")
    val sc = new SparkContext(conf)
  }
}
