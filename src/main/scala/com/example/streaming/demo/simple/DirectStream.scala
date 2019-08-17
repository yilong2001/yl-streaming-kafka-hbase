package com.example.streaming.demo.simple

import java.util
import java.util.Arrays
import java.util.concurrent.ConcurrentLinkedQueue

import com.example.streaming.demo.data.{AvroDataBuilder, KafkaDataBuilder}
import com.example.streaming.demo.simple.KafkaConsumerOnly.getRecord
import com.example.streaming.util.StrUtils
import org.apache.avro.Schema
import org.apache.commons.logging.LogFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.Function
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yilong on 2019/3/20.
  */
class MyDefaultPerPartitionConfig(conf: SparkConf)
  extends PerPartitionConfig {
  val maxRate = conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
  val minRate = conf.getLong("spark.streaming.kafka.minRatePerPartition", 1)

  def maxRatePerPartition(topicPartition: TopicPartition): Long = maxRate
  override def minRatePerPartition(topicPartition: TopicPartition): Long = minRate
}

object DirectStream {
  val topic1 = "myrecord1"
  val groupId = "myrecord1.groupid.006"
  val clientid = "myrecord1.clientid.010"
  val table1 = "table1"

  val kafkaParams = KafkaDataBuilder.getKafkaProp(groupId, clientid)
  private val LOG = LogFactory.getLog("MainLog")

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

    val kpMap = new util.HashMap[String,Object]()
    kafkaParams.foreach(x => kpMap.put(x._1, x._2))

    val list = new util.ArrayList[String]()
    list.add(topic1)

    val ssc = new StreamingContext(spark.sparkContext, Milliseconds(5000))

    val stream = org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, Array[Byte]](list, kpMap),
      new MyDefaultPerPartitionConfig(spark.sparkContext.getConf))

    val stream1 = stream.transform({ rdd =>
      LOG.info(s"************************************ : stream.transform: ")

      // Get the offset ranges in the RDD
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(record => {
        val parser = new Schema.Parser();
        val schema = parser.parse(AvroDataBuilder.getComposedSchemaStr());
        val rd = getRecord(schema, record.value())
        println(rd)
        StrUtils.avroUtf8ToString(rd.get("table"))
      })
    })

    stream1.foreachRDD { rdd =>
      LOG.info(s"************************************ : stream1.foreachRDD ")

      rdd.zipWithIndex().collect().foreach {x =>
        LOG.info(s"************************************ : rdd.foreach ${x} ")
      }

      for (o <- offsetRanges) {
        LOG.info(s"--------------------------- : topic ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      val collected = rdd.mapPartitionsWithIndex { (i, iter) =>
        // For each partition, get size of the range in the partition,
        // and the number of items in the partition
        val off = offsetRanges(i)
        val all = iter.toSeq
        val partSize = all.size
        val rangeSize = off.untilOffset - off.fromOffset
        Iterator((partSize, rangeSize))
      }

      // Verify whether number of elements in each partition
      // matches with the corresponding offset range
      collected.foreach { case (partSize, rangeSize) =>
        LOG.info(s"--------------------------- : partSize:${partSize}, rangeSize:${rangeSize}")
      }

      LOG.info(s"************************************ : rdd.count ${rdd.count()}")
    }

    //stream1.foreachRDD { rdd => println(s"************************************ : ${rdd.count()}") }

    ssc.start()

    ssc.awaitTermination()
  }

}
