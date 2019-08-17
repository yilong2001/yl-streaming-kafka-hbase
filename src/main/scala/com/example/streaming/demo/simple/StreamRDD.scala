package com.example.streaming.demo.simple

import java.util
import java.util.{HashMap, Map}

import com.example.streaming.demo.data.{AvroDataBuilder, KafkaDataBuilder}
import com.example.streaming.demo.simple.KafkaConsumerOnly.getRecord
import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.Function
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

/**
  * Created by yilong on 2019/3/20.
  */
object StreamRDD {
  val topic1 = "myrecord1"
  val groupId = "myrecord1.groupid.005"
  val clientid = "myrecord1.clientid.010"
  val table1 = "table1"

  val kafkaParams = KafkaDataBuilder.getKafkaProp(groupId, clientid)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("KafkaStreamingDemo1")
      .master("spark://localhost:7077")
      .config("spark.streaming.kafka.consumer.cache.enabled",false)
      .config("spark.local.dir","~/setup/tmp/spark2.4/tmp")
      .config("spark.work.dir", "~/setup/tmp/spark2.4/work")
      .getOrCreate()

    import scala.collection.JavaConversions._

    val offsetRanges: Array[OffsetRange] = Array(OffsetRange.create(topic1, 0, 0, 9999999)/*,
      OffsetRange.create(topic1, 1, 0, 9999999),
      OffsetRange.create(topic1, 2, 0, 9999999)*/)

    val leaders: util.Map[TopicPartition, String] = new util.HashMap[TopicPartition, String]
    val hostAndPort: Array[String] = Array("localhost", "9092")
    val broker: String = hostAndPort(0)
    leaders.put(offsetRanges(0).topicPartition, broker)
    //leaders.put(offsetRanges(1).topicPartition, broker)
    //leaders.put(offsetRanges(2).topicPartition, broker)

    val handler: Function[ConsumerRecord[String, Array[Byte]], String] = new Function[ConsumerRecord[String, Array[Byte]], String]() {
      override def call(r: ConsumerRecord[String, Array[Byte]]): String = {
        val parser = new Schema.Parser();
        val schema = parser.parse(AvroDataBuilder.getComposedSchemaStr());
        val rd = getRecord(schema, r.value())
        println(rd)
        r.key()
      }
    }

    val kpMap = new util.HashMap[String,Object]()
    kafkaParams.foreach(x => kpMap.put(x._1, x._2))

    val rdd1: RDD[String] = org.apache.spark.streaming.kafka010.KafkaUtils.createRDD[String, Array[Byte]](spark.sparkContext,
      kpMap,
      offsetRanges,
      LocationStrategies.PreferFixed(leaders)).map(record => {
        val parser = new Schema.Parser();
        val schema = parser.parse(AvroDataBuilder.getComposedSchemaStr());
        val rd = getRecord(schema, record.value())
        println(rd)
        record.key()
    })

    rdd1.foreach(x => {
      println(s"------------------------------ : ${x}")
    })


  }

}
