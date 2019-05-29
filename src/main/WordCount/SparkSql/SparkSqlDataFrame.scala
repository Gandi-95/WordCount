package SparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://www.cnblogs.com/qingyunzong/p/8987579.html
  */
object SparkSqlDataFrame {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("reflex").setMaster("local")
    val sc = new SparkContext(conf)


//    reflex(sc)
    structTypeCreat(sc)
  }


  /**
    * 通过 case class 创建 DataFrames（反射）
    */
  case class People(var name:String,var age:Int)
  def reflex(sc:SparkContext): Unit ={
    val sql = new SQLContext(sc)

    val rdd = sc.parallelize(Array(("xiaoMing",23),("xiaoWang",12),("Andy",63)))
    import sql.implicits._
    // 将RDD 转换成 DataFrames
    val PeopleDF = rdd.toDF()
    //将DataFrames创建成一个临时的视图
    PeopleDF.createOrReplaceTempView("people")
    //使用SQL语句进行查询
    sql.sql("select * from people").show()
  }


  /*
   * structType 创建 DataFrames
   */
  def structTypeCreat(sc:SparkContext): Unit ={
    val sql = new SQLContext(sc)
    val rdd = sc.parallelize(Array(("xiaoMing",23),("xiaoWang",12),("Andy",63)))
    // 将 RDD 数据映射成 Row
    val rowRDD: RDD[Row] = rdd.map(x=>Row(x._1,x._2))

    // 创建 StructType 来定义结构
    val structType: StructType = StructType(
      //字段名，字段类型，是否可以为空
      StructField("name", StringType, true) :: StructField("age", IntegerType, true) :: Nil
    )
    val peopleDF = sql.createDataFrame(rowRDD,structType)
    peopleDF.createOrReplaceTempView("people")
    sql.sql("select * from people").show()
  }



}
