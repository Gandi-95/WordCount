package SparkSql

import org.apache.spark.sql.SparkSession

object DataFrameAPi {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApi").master("local[2]").getOrCreate()
    val peopleDF = spark.read.json("D:\\Intellij Workspace\\WordCount\\src\\main\\WordCount\\SparkSql\\data\\peple.json")

    peopleDF.printSchema()
    peopleDF.show()

    //查询列
    peopleDF.select("name").show()
    peopleDF.select("name","age").show()
    //查询几列的数据，并进行计算
    peopleDF.select(peopleDF.col("name"),(peopleDF.col("age")+10).as("age2")).show()
    //对某一列的值进行过滤
    peopleDF.filter(peopleDF.col("age")>19).show()
    //对某一列进行分组再聚合
    peopleDF.groupBy(peopleDF.col("age")).count().show()




    spark.stop()
  }
}
