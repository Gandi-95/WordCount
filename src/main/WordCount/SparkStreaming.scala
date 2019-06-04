import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreming").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10))

    val ddr = ssc.textFileStream("D:\\IdeaProjects\\WordCount\\src\\main\\WordCount\\Test\\data")
    val wordCounts = ddr.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
