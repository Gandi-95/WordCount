package SparkStreaming.Kafka

import java.util

import SparkStreaming.StreamingExamples
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

//spark-submit --driver-class-path /usr/local/spark/jars/*:/usr/local/spark/jars/kafka/* --class "SparkStreaming.Kafka.KafKaWrodProducer" KafkaWrodProducer.jar 127.0.0.1:9092 gandi 3 4
object KafKaWrodProducer {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KafKaWrodProducer <brokers> <topics> <MessagesPerSec> <WordsPerMessage>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()
    /* 输入的四个参数分别代表着
       * 1. brokers 为zookeeper地址
       * 2. topic该消费者所消费的topics
       * 3. messagesPerSec1秒发送几条消息
       * 4. wordsPerMessage一次发送几个word
       */
    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String,String](props)

    while (true){
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x =>
          Random.nextInt(10).toString).mkString(" ")
        println(str)
        val message = new ProducerRecord[String,String](topic,null,str)
        producer.send(message)
      }
      Thread.sleep(1000)
    }
  }

}
