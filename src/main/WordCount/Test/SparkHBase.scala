package Test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object SparkHBase {

  def main(args: Array[String]): Unit = {

//    read()
    write()
  }


  def read(): Unit ={
    val configuration = HBaseConfiguration.create()
    val conf = new SparkConf().setAppName("SparkWriteHBase").setMaster("local")
    val sc = new SparkContext(conf)
    configuration.set(TableInputFormat.INPUT_TABLE,"student1")
    val stuRdd = sc.newAPIHadoopRDD(configuration,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
    val count = stuRdd.count()
    println("count:"+count)
    stuRdd.cache()

    stuRdd.foreach({
      case(_,result)=>
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
        val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
        val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
        println("Row key:"+key+" name:"+name+" gender:"+gender+" age:"+age)
    })
  }


  def write(): Unit ={
    val conf = new SparkConf().setAppName("SparkWriteHBase").setMaster("local")
    val sc = new SparkContext(conf)
    val tableName = "student1"
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val indatatRdd = sc.makeRDD(Array("3,wang,M,34","4,chen,M,22"))
    val rdd = indatatRdd.map(_.split(",")).map(arr=>{
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3)))
      (new ImmutableBytesWritable,put)
    })
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
