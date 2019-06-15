package SparkStreaming


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel

/**
  * window 使用nc: nc -v -l -p 9999 | nc -v -l -p port < 文件
  *                nc -v -n ip port
  * 通过DataSourceSocket将文件数据写入9999端口，实现套接字流
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    val host = "localhost"
    val cmd = 9999

    StreamingExamples.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("NetWorkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))
    val lines = ssc.socketTextStream(host,cmd,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x=>(x,1)).reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
