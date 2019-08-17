package com.example.streaming.demo.simple

import java.util.Locale

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import java.util

import com.example.streaming.demo.data.{AvroDataBuilder, KafkaDataBuilder}
import com.example.streaming.demo.simple.DirectStream.kafkaParams
import com.example.streaming.demo.simple.KafkaConsumerOnly
import com.example.streaming.util.StrUtils
import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.OffsetRange
/**
  * Created by yilong on 2019/8/16.
  */
object KafkaMeta {
  def onStart[K,V](currentOffsets: java.util.Map[TopicPartition, Long],
                   kafkaParams: java.util.Map[String,Object],
                   topics: java.util.Collection[String],
                   offsets: java.util.Map[TopicPartition, Long]): Consumer[K, V] = {

    val consumer = new KafkaConsumer[K, V](kafkaParams)
    consumer.subscribe(topics)
    val toSeek = if (currentOffsets.isEmpty) {
      offsets
    } else {
      currentOffsets
    }

    if (!toSeek.isEmpty) {
      // work around KAFKA-3370 when reset is none
      // poll will throw if no position, i.e. auto offset reset none and no explicit position
      // but cant seek to a position before poll, because poll is what gets subscription partitions
      // So, poll, suppress the first exception, then seek
      val aor = kafkaParams.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      val shouldSuppress =
        aor != null && aor.asInstanceOf[String].toUpperCase(Locale.ROOT) == "NONE"
      try {
        consumer.poll(0)
      } catch {
        case x: NoOffsetForPartitionException if shouldSuppress =>
          println("Catching NoOffsetForPartitionException since " +
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + " is none.  See KAFKA-3370")
      }

      toSeek.foreach { case (topicPartition, offset) =>
        consumer.seek(topicPartition, offset)
      }

      // we've called poll, we must pause or next poll may consume messages and set position
      consumer.pause(consumer.assignment())
    }

    consumer
  }

  def myseek[K,V](kc: KafkaConsumer[K,V], tp: TopicPartition, offset: Long): Unit = {
    println(s"Seeking to $tp $offset")
    kc.seek(tp, offset)
  }

  private def mypoll[K,V](kc: KafkaConsumer[K,V], tp: TopicPartition, timeout: Long):
  util.List[ConsumerRecord[K,V]] = {
    val p = kc.poll(timeout)
    val r = p.records(tp)
    print(s"Polled ${p.partitions()}  ${r.size}")

    r
  }

  def createConsumer[K,V](kafkaParams: java.util.Map[String,Object],
                          topicPartition: TopicPartition): KafkaConsumer[K, V] = {
    val c = new KafkaConsumer[K, V](kafkaParams)
    val topics = java.util.Arrays.asList(topicPartition)
    c.assign(topics)
    c

//    myseek(c, topicPartition, 1000)
//
//    val out = mypoll(c, topicPartition, 100)
//
//    out.foreach(x => {
//      val parser = new Schema.Parser();
//      val schema = parser.parse(AvroDataBuilder.getComposedSchemaStr());
//      val rd = KafkaConsumerOnly.getRecord(schema, x.value().asInstanceOf[Array[Byte]])
//      //println(rd)
//      val tb = StrUtils.avroUtf8ToString(rd.get("table"))
//      println(s"----------- : ${tb}")
//    })

    c
  }

  val topic1 = "myrecord1"
  val groupId = "myrecord1.groupid.006"
  val clientid = "myrecord1.clientid.100"
  val table1 = "table1"

  val kafkaParams = KafkaDataBuilder.getKafkaProp(groupId, clientid)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("KafkaStreamingDemo1")
      .master("spark://localhost:7077")
      .config("spark.streaming.kafka.consumer.cache.enabled", false)
      .config("spark.local.dir", "~/setup/tmp/spark2.4/tmp")
      .config("spark.work.dir", "~/setup/tmp/spark2.4/work")
      .getOrCreate()

    val offsetRanges: Array[OffsetRange] = Array(OffsetRange.create(topic1, 0, 0, 9999999)/*,
      OffsetRange.create(topic1, 1, 0, 9999999),
      OffsetRange.create(topic1, 2, 0, 9999999)*/)

    val leaders: util.Map[TopicPartition, String] = new util.HashMap[TopicPartition, String]
    val hostAndPort: Array[String] = Array("localhost", "9092")
    val broker: String = hostAndPort(0)
    leaders.put(offsetRanges(0).topicPartition, broker)
    //leaders.put(offsetRanges(1).topicPartition, broker)
    //leaders.put(offsetRanges(2).topicPartition, broker)

    val curOffsets = new util.HashMap[TopicPartition, Long]()
    curOffsets.put(offsetRanges(0).topicPartition, 0)
    //curOffsets.put(offsetRanges(1).topicPartition, 0)
    //curOffsets.put(offsetRanges(2).topicPartition, 0)

    val list = new util.ArrayList[String]()
    list.add(topic1)

    val kpMap = new util.HashMap[String,Object]()
    kafkaParams.foreach(x => kpMap.put(x._1, x._2))

    def getBrokers = {
      val c = onStart(curOffsets, kpMap, list, curOffsets)

      val result = new util.HashMap[TopicPartition, String]()
      val hosts = new util.HashMap[TopicPartition, String]()
      val assignments = c.assignment().iterator()
      while (assignments.hasNext()) {
        val tp: TopicPartition = assignments.next()
        if (null == hosts.get(tp)) {
          val infos = c.partitionsFor(tp.topic).iterator()
          while (infos.hasNext()) {
            val i = infos.next()
            hosts.put(new TopicPartition(i.topic(), i.partition()), i.leader.host())
          }
        }
        result.put(tp, hosts.get(tp))
      }
      result
    }

    val parts = getBrokers
    val partsets = parts.entrySet()
    //kpMap.put("client.id", "consumer-1")
    kpMap.put("group.id", "spark-executor-"+kpMap.get("group.id").toString)


    val kconsumers = partsets.map(x => {
      //createConsumer[String,Array[Byte]](kpMap, x.getKey)
    })

    val kafkaconsumer = createConsumer[String,Array[Byte]](kpMap, partsets.head.getKey)
    KafkaConsumerOnly.comsumerRecord(AvroDataBuilder.getComposedSchemaStr(), kafkaconsumer, topic1)

    kconsumers.foreach(kc => {
      //KafkaConsumerOnly.comsumerRecord(AvroDataBuilder.getComposedSchemaStr(), kc, topic1)
    })
  }
}
