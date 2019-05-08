package Test

//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.HTable
//import org.apache.hadoop.mapreduce.Job
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.BasicConfigurator

object WriteHBase {
  val tablename = "student"
  val configuration = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(configuration)
  val admin = connection.getAdmin
  //  val table = connection.getTable(TableName.valueOf(tablename))


  def main(args: Array[String]): Unit = {
    //    val sparkConf = new SparkConf().setAppName("WriteHBase").setMaster("local")
    //    val sc = new SparkContext(sparkConf)
    //
    //    val tablename = "student"
    //    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,tablename)
    //    val job = new Job(sc.hadoopConfiguration)
    //    job.setOutputFormatClass(classOf[ImmutableBytesWritable])

    BasicConfigurator.configure()

    createTable(tablename, Array("info"))
    inserTable(tablename, "1", "info", "name", "xiaoming")
    inserTable(tablename, "1", "info", "gender", "F")
    inserTable(tablename, "1", "info", "age", "23")

    getValue(tablename,"1","info","name")
  }

  def createTable(tableName: String, columnFamilys: Array[String]): Unit = {
    //操作的表名
    val tName = TableName.valueOf(tableName)
    if (!admin.tableExists(tName)) {
      //创建Hbase表模式
      val descriptor = new HTableDescriptor(tName)
      //创建列簇i
      columnFamilys.foreach(s => descriptor.addFamily(new HColumnDescriptor(s)))
      //创建表
      admin.createTable(descriptor)
      println("create table successful!")
    }
  }

  def inserTable(tName: String, rowkey: String, columnFamily: String, column: String, value: String): Unit = {
    try{
      val table = connection.getTable(TableName.valueOf(tName))
      //准备key 的数据
      val puts = new Put(rowkey.getBytes())
      //添加列簇名,字段名,字段值value
      puts.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes())
      table.put(puts)
      table.close()
    }

  }

  def getValue(tName: String, rowkey: String, columnFamily: String, column: String): Unit = {
    val table:Table = null
    try{
      val table = connection.getTable(TableName.valueOf(tName))
      val g = new Get(rowkey.getBytes())
      val r = table.get(g)
      val value = Bytes.toString(r.getValue(columnFamily.getBytes(),column.getBytes()))
      print("key:"+value)
    }finally {
      if (table!=null) table.close()
    }

  }


}
