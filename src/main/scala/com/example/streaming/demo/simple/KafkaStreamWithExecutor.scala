package com.example.streaming.demo.simple

import java.util
import java.util.UUID
import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor}

import com.example.streaming.demo.data.{AvroDataBuilder, KafkaDataBuilder}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{ForeachAction, KStreamBuilder, Predicate}

import scala.concurrent.ExecutionContext


/**
  * Created by yilong on 2019/3/17.
  */

object KafkaStreamWithExecutor extends App {
  def getRecord(schema: Schema, message: Array[Byte]) : GenericRecord = {
    // Deserialize and create generic record
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val userData: GenericRecord = reader.read(null, decoder)

    userData
  }

  def comsumerMyRecord(schemaStr : String, consumer : KafkaConsumer[String,Array[Byte]], topic : String) : Unit = {
    val parser = new Schema.Parser();
    val schema = parser.parse(schemaStr);

    val minBatchSize = 10
    while (true) {
      val records = consumer.poll(100)
      var i = 0
      import scala.collection.JavaConversions._
      for (record <- records) {
        printf("offset = %d, key = %s, value = %s \n", record.offset, record.key, record.value)
        i += 1
      }

      //批量完成写入后，手工sync offset
      if (i >= minBatchSize) consumer.commitSync
    }
  }

  val topic = "myrecord2"
  val brokers = "localhost:9092"

  val groupId = UUID.randomUUID().toString
  val schema = AvroDataBuilder.getSimpleSchema()
  val props = KafkaDataBuilder.getKafkaProp(groupId, "")

  import org.apache.kafka.clients.consumer.KafkaConsumer

  val consumer = new KafkaConsumer[String, Array[Byte]](props)
  consumer.assign(util.Arrays.asList(new TopicPartition(topic, 0)))

  val builder = new KStreamBuilder()
  val source = builder.stream[String,Array[Byte]](topic)

  source.filter(new Predicate[String,Array[Byte]] {
    override def test(k: String, v: Array[Byte]): Boolean = {
      println("********************* : test --------- "+k)
      //println(getRecord(schema, v))
      return true
    }
  })

  source.foreach(new ForeachAction[String,Array[Byte]] {
    override def apply(k: String, v: Array[Byte]): Unit = {
      println("********************* : foreach --------- "+k)
      println(getRecord(schema, v))
    }
  })

  val executionContext = ExecutionContext.fromExecutorService(newDaemonThreadScheduledExecutor("debug", 2))

  val streams = new KafkaStreams(builder, props)

  executionContext.execute(new Runnable {
    override def run(): Unit = {
      streams.start()
    }
  })

  executionContext.shutdown()

  def newDaemonThreadScheduledExecutor(threadName: String, nums : Int): ScheduledExecutorService = {
    val executor = new ScheduledThreadPoolExecutor(nums, new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build())
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

}

