package org.apache.kafka.clients.consumer

import java.util
import java.util.Map.Entry

import com.example.streaming.demo.data.KafkaDataBuilder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.common.serialization.StringDeserializer


/**
  * Created by yilong on 2019/3/17.
  */

object KafkaConsumerOnly extends App {
  def getRecord(schema: Schema, message: Array[Byte]) : GenericRecord = {
    // Deserialize and create generic record
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val userData: GenericRecord = reader.read(null, decoder)

    userData
  }

  def comsumerRecord(schemaStr : String, consumer : KafkaConsumer[String,Array[Byte]], topic : String) : Unit = {
    val parser = new Schema.Parser();
    val schema = parser.parse(schemaStr);

    val minBatchSize = 10
    while (true) {
      val records = consumer.poll(100)
      var i = 0
      import scala.collection.JavaConversions._
      for (record <- records) {
        printf("offset = %d, key = %s, \n", record.offset, record.key)
        val rd = getRecord(schema, record.value())
        println(rd)
        i += 1
      }

      println("********************** : " + i)
      if (i >= minBatchSize) consumer.commitSync //批量完成写入后，手工sync offset
    }
  }

  val topic = "myrecord1"
  val brokers = "localhost:9092"
  val groupId = "myrecord1.groupid.008" // UUID.randomUUID().toString
  val clientid = ""

  val props = KafkaDataBuilder.getKafkaProp(groupId, clientid)

  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.GROUP_ID_CONFIG -> groupId,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    //"sasl.mechanism" -> "PLAIN",
  "security.protocol" -> "PLAINTEXT")

  val kpmap = new util.HashMap[String, Object]()
  kafkaParams.foreach(x => kpmap.put(x._1, x._2))

  val sconfig = new ConsumerConfig(kpmap, true)
  val size = sconfig.values().size()
  val keys1 = sconfig.values().keySet()

  val it1 = keys1.iterator()
  while (it1.hasNext()) {
    val key = it1.next()

    print(key)
    print("---")
    println(sconfig.values().get(key))
  }

  val consumer = new KafkaConsumer[String,Array[Byte]](props)
  consumer.subscribe(util.Arrays.asList(topic))

  //comsumerRecord(AvroDataBuilder.getComposedSchemaStr(), consumer, topic)
}
