package com.example.streaming.demo.simple

import com.example.streaming.demo.data.KafkaDataBuilder
import com.example.streaming.myudf.{IsAbnormal, IsAbnormal2}
import com.example.streaming.pipeline.{RegMacro, UDFMetaData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types._

/**
  * Created by yilong on 2019/3/20.
  */
object StructuredStreamSqlUdf {

  def udfMetasToString(udfMetas:Array[UDFMetaData]) : String = {
    udfMetas.map(udfmd => {
      "spark.udf.register(\""+udfmd.nickName+"\", "+udfmd.staticFuncName+" _)"
    }).mkString(";")
  }

  def registerSelfUDF(spark: SparkSession, udfMetas:Array[UDFMetaData]) = {
    import com.example.streaming.pipeline.RegMacro._
    val regStr = udfMetasToString(udfMetas)
    println(regStr)
    //RegMacro(spark, regStr)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("KafkaStreamingDemo1")
      .master("local[4]")
      .config("spark.sql.warehouse.dir","/Users/yilong/work/bigdata/code/mygithub/yl-streming_2_x_kafka_hbase/spark-warehouse")
      .config("hive.metastore.warehouse.dir", "/Users/yilong/work/bigdata/code/mygithub/yl-streming_2_x_kafka_hbase/spark-warehouse")
      .getOrCreate()

    import scala.collection.JavaConversions._

    val userSchema = new StructType().add("id", "int").add("name", "string").add("grade","int")
    val df = spark
      .readStream
      .option("sep", ",")
      .option("header","true")
      .schema(userSchema)
      .csv("/Users/yilong/work/bigdata/code/mygithub/yl-streming_2_x_kafka_hbase/mydebug")    // Equivalent to format("csv").load("/path/to/directory")

    df.createOrReplaceGlobalTempView("user_table")

    val udfMetas = Array{UDFMetaData("isAbnormalEx1", "com.example.streaming.myudf.IsAbnormal.isAbnormalEx")}
    registerSelfUDF(spark, udfMetas)

    spark.udf.register("isAbnormalEx2", new IsAbnormal2().isAbnormalEx _)
    val df2 = spark.sql("select grade, isAbnormalEx2(grade, name) from global_temp.user_table")

    val qf = df2.writeStream.format("console").start()

    qf.awaitTermination()
  }

}
