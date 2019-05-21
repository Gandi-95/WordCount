package Test

import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondSort").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("file:////home/gandi/IdeaProjects/WordCount/src/main/WordCount/Test/data/file",1)
    rdd.map(line=>(new SecondKeySort(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line)).sortByKey(false).map(sortKey=>sortKey._2).collect().foreach(println)
  }


  class SecondKeySort(val frist:Int,val second:Int) extends Ordered[SecondKeySort] with Serializable{

    override def compare(that: SecondKeySort): Int ={
      if(this.frist!=that.frist){
        this.frist - that.frist
      }else{
        this.second - that.second
      }
    }
  }
}
