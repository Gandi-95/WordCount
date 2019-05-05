package Test

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object RDDTest {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("PartitionerTest").setMaster("local")
      val sc = new SparkContext(conf)

      TuShu(conf,sc)



  }

  def TuShu(conf:SparkConf,sc:SparkContext):Unit={
    val rdd = sc.parallelize(Array(("spark",2),("hadoop",6),
      ("hadoop",4),("spark",6)))
    rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>(x._1/x._2))
      .collect().foreach(println)
  }

}
