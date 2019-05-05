import org.apache.spark.{Partitioner, SparkConf, SparkContext}

class PartitionerTest(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    key.toString.toInt % 10
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    println(1 to 10)

    //rdd fengqu

    val conf = new SparkConf().setAppName("PartitionerTest").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(1 to 10, 5)
    data.map((_, 1)).partitionBy(new PartitionerTest(10)).map(_._1)
      .saveAsTextFile("file:///home/gandi/IdeaProjects/WordCount/out/partitonertest")

  }
}