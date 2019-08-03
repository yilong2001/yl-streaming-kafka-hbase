package com.example.streaming.demo.simple

import com.example.streaming.demo.data.KafkaDataBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * Created by yilong on 2019/3/20.
  */
object StructuredStreamSql {
  val topic1 = "myrecord1"
  val groupId = "myrecord1.groupid.005"
  val clientid = "myrecord1.clientid.010"
  val table1 = "table1"

  val props = KafkaDataBuilder.getKafkaProp(groupId, clientid)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("KafkaStreamingDemo1")
      .master("local[4]")
      .getOrCreate()

    import scala.collection.JavaConversions._

    val ss = spark.readStream
      .format("kafka")
      .options(props)
      .option("subscribe", topic1)
      .option("startingOffsets", "earliest")
      .load()

    ss.createOrReplaceGlobalTempView(table1)

    val qs2 = ss.writeStream.trigger(ProcessingTime(5)).outputMode("append").format("console").start()

    qs2.awaitTermination()
  }

}
