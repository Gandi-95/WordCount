package SparkStreaming.Kafka

import SparkStreaming.StreamingExamples
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()
    /* 输入的四个参数分别代表着
    * 1. zkQuorum 为zookeeper地址
    * 2. group为消费者所在的组
    * 3. topics该消费者所消费的topics
    * 4. numThreads开启消费topic线程的个数
    */
    val Array(zkQuorum, group, topics, numThreads) = args
    val conf = new SparkConf().setMaster("KafkaWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint(".")
    // 将topics转换成topic-->numThreads的哈稀表
    //    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val topicsArr = topics.split(",")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> zkQuorum,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /**
      * createStream是Spark和Kafka集成包0.8版本中的方法，它是将offset交给ZK来维护的
      *
      * 在0.10的集成包中使用的是createDirectStream，它是自己来维护offset，
      * 速度上要比交给ZK维护要快很多，但是无法进行offset的监控。
      * 这个方法只有3个参数，使用起来最为方便，但是每次启动的时候默认从Latest offset开始读取，
      * 或者设置参数auto.offset.reset="smallest"后将会从Earliest offset开始读取。
      *
      * 官方文档@see <a href="http://spark.apache.org/docs/2.1.2/streaming-kafka-0-10-integration.html">Spark Streaming + Kafka Integration Guide (Kafka broker version 0.10.0 or higher)</a>
      *
      */
    val lines = KafkaUtils.createDirectStream[String, String](ssc,
      PreferConsistent,
      Subscribe[String, String](topicsArr, kafkaParams))

  }
}
