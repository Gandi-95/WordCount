package com.imooc.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()
    val accessRDD = spark.sparkContext.textFile("E:\\Spark\\imoocLog\\access.log")
    //    accessRDD.take(10).foreach(println)

    val accessDF = spark.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)),
      AccessConvertUtil.struct)

//    accessDF.printSchema()
//    accessDF.show()

    accessDF.write.format("json").mode(SaveMode.Overwrite).save("D:\\IntellijWorkspace\\WordCount\\src\\main\\imooc\\com\\imooc\\spark\\data")
//    accessDF.write.format("parquet").mode(SaveMode.Overwrite)
//      .partitionBy("day").save("D:\\IntellijWorkspace\\WordCount\\src\\main\\imooc\\com\\imooc\\spark\\data")



    spark.stop()
  }
}
